export type RpcId = number;

export type RpcEnvelope = {
  id?: RpcId | null;
  ok?: boolean;
  result?: unknown;
  error?: { code?: string; message?: string };
  method?: string;
  params?: unknown;
  notification?: string;
  seq?: number;
  event?: unknown;
};

export type DaemonEvent = {
  seq?: number;
  type: string;
  eventType?: string;
  timestamp?: string;
  runId?: string;
  taskId?: string;
  agentId?: string;
  payload?: Record<string, unknown>;
  [key: string]: unknown;
};

type Pending = { resolve: (value: unknown) => void; reject: (reason: Error) => void; timer: number };

export class DaemonRpc {
  private socket?: WebSocket;
  private pending = new Map<RpcId, Pending>();
  private listeners = new Set<(event: DaemonEvent) => void>();
  private sequence = 0;
  private reconnectTimer?: number;
  private reconnectAttempt = 0;
  private intentionalClose = false;
  private highestEventSeq = 0;
  private readonly endpoint: string;
  onState?: (state: "connecting" | "online" | "offline") => void;

  get lastEventSeq() { return this.highestEventSeq; }

  constructor(endpoint = import.meta.env.AGENTOS_WS_ADDR || "ws://127.0.0.1:8741") {
    this.endpoint = endpoint;
  }

  connect() {
    if (this.socket?.readyState === WebSocket.OPEN || this.socket?.readyState === WebSocket.CONNECTING) return;
    this.intentionalClose = false;
    this.onState?.("connecting");
    try {
      const socket = new WebSocket(this.endpoint);
      this.socket = socket;
      socket.onopen = () => {
        this.reconnectAttempt = 0;
        this.onState?.("online");
      };
      socket.onmessage = (message) => this.handleMessage(message.data);
      socket.onerror = () => socket.close();
      socket.onclose = () => {
        if (this.socket === socket) this.socket = undefined;
        this.onState?.("offline");
        if (!this.intentionalClose) this.scheduleReconnect();
      };
    } catch {
      this.onState?.("offline");
      this.scheduleReconnect();
    }
  }

  close() {
    this.intentionalClose = true;
    window.clearTimeout(this.reconnectTimer);
    this.socket?.close();
    this.pending.forEach(({ reject, timer }) => { window.clearTimeout(timer); reject(new Error("Daemon connection closed")); });
    this.pending.clear();
  }

  request<T>(method: string, params: Record<string, unknown> = {}, timeout = 12_000): Promise<T> {
    if (this.socket?.readyState !== WebSocket.OPEN) return Promise.reject(new Error("Daemon is offline"));
    const id = ++this.sequence;
    return new Promise<T>((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out`));
      }, timeout);
      this.pending.set(id, { resolve: (value) => resolve(value as T), reject, timer });
      this.socket?.send(JSON.stringify({ id, method, params }));
    });
  }

  subscribe(listener: (event: DaemonEvent) => void) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private handleMessage(raw: unknown) {
    if (typeof raw !== "string") return;
    let message: RpcEnvelope;
    try { message = JSON.parse(raw) as RpcEnvelope; } catch { return; }
    if (typeof message.id === "number") {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      window.clearTimeout(pending.timer);
      if (message.ok === false || message.error) pending.reject(new Error(message.error?.message || "RPC request failed"));
      else pending.resolve(message.result);
      return;
    }
    if (message.notification === "event" && message.event && typeof message.event === "object") {
      const wire = message.event as DaemonEvent;
      const event = { ...wire, type: wire.type || wire.eventType || "event.unknown", seq: typeof message.seq === "number" ? message.seq : wire.seq };
      if (typeof event.seq === "number") this.highestEventSeq = Math.max(this.highestEventSeq, event.seq);
      this.listeners.forEach((listener) => listener(event));
      return;
    }
    const eventType = message.method;
    if (eventType) this.listeners.forEach((listener) => listener({ ...(message.params as DaemonEvent), type: eventType }));
  }

  private scheduleReconnect() {
    window.clearTimeout(this.reconnectTimer);
    const delay = Math.min(10_000, 500 * 2 ** this.reconnectAttempt++);
    this.reconnectTimer = window.setTimeout(() => this.connect(), delay);
  }
}

export const rpc = new DaemonRpc();
