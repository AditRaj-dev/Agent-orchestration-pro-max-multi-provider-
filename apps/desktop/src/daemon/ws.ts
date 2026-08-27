// F-11 Desktop WebSocket Client (docs/F-11-desktop.md §2, §3.2, §4)
import type {
  ConnectionState,
  DaemonNotification,
  DaemonRequest,
  DaemonResponse,
  EventWire,
} from "../types";

export interface DaemonWsClientOptions {
  url?: string;
  WebSocketClass?: typeof WebSocket;
  autoConnect?: boolean;
  pingIntervalMs?: number;
  pingTimeoutMs?: number;
  initialReconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
}

export interface ConnectionMeta {
  state: ConnectionState;
  url: string;
  latencyMs: number | null;
  lastPingAt: string | null;
  lastSeq: number;
  reconnectAttempts: number;
  error: string | null;
}

export const DEFAULT_WS_ADDR = "ws://127.0.0.1:8741";
export const STORAGE_KEY_WS_URL = "agentos_ws_addr";

/**
 * Resolves WS address with the following precedence:
 * 1. `?ws=` URL query parameter
 * 2. `localStorage.getItem("agentos_ws_addr")`
 * 3. `import.meta.env.AGENTOS_WS_ADDR` or `import.meta.env.VITE_AGENTOS_WS_ADDR`
 * 4. Default fallback: `ws://127.0.0.1:8741`
 */
export function resolveWsAddress(customOverride?: string): string {
  if (customOverride && customOverride.trim()) {
    return normalizeWsUrl(customOverride.trim());
  }

  // 1. Query parameter
  if (typeof window !== "undefined" && window.location?.search) {
    try {
      const params = new URLSearchParams(window.location.search);
      const queryWs = params.get("ws");
      if (queryWs && queryWs.trim()) {
        return normalizeWsUrl(queryWs.trim());
      }
    } catch {
      // ignore
    }
  }

  // 2. Local storage
  if (typeof window !== "undefined" && window.localStorage) {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY_WS_URL);
      if (stored && stored.trim()) {
        return normalizeWsUrl(stored.trim());
      }
    } catch {
      // ignore
    }
  }

  // 3. Environment variables
  const env = (import.meta as any)?.env;
  if (env) {
    const envAddr = env.AGENTOS_WS_ADDR || env.VITE_AGENTOS_WS_ADDR;
    if (envAddr && typeof envAddr === "string" && envAddr.trim()) {
      return normalizeWsUrl(envAddr.trim());
    }
  }

  // 4. Default fallback
  return DEFAULT_WS_ADDR;
}

export function normalizeWsUrl(input: string): string {
  if (!input) return DEFAULT_WS_ADDR;
  let url = input.trim();
  if (!url.startsWith("ws://") && !url.startsWith("wss://")) {
    url = `ws://${url}`;
  }
  return url;
}

export class DaemonWsClient {
  private ws: WebSocket | null = null;
  private WebSocketClass: typeof WebSocket;
  private url: string;
  private state: ConnectionState = "disconnected";
  private reconnectAttempts = 0;
  private reconnectTimer: any = null;
  private pingTimer: any = null;
  private pingTimeoutTimer: any = null;
  private nextRequestId = 1;
  private pendingRequests = new Map<
    number | string,
    {
      resolve: (value: any) => void;
      reject: (reason: any) => void;
      timeoutId: any;
      method: string;
    }
  >();

  private subscribers = new Set<(event: EventWire) => void>();
  private stateListeners = new Set<(state: ConnectionState, meta: ConnectionMeta) => void>();

  private lastSeq = 0;
  private latencyMs: number | null = null;
  private lastPingAt: string | null = null;
  private lastError: string | null = null;
  private activeSubscriptionId: string | null = null;
  private isExplicitlyClosed = false;

  private readonly pingIntervalMs: number;
  private readonly pingTimeoutMs: number;
  private readonly initialReconnectDelayMs: number;
  private readonly maxReconnectDelayMs: number;

  constructor(options: DaemonWsClientOptions = {}) {
    this.WebSocketClass = options.WebSocketClass || (typeof WebSocket !== "undefined" ? WebSocket : (null as any));
    this.url = resolveWsAddress(options.url);
    this.pingIntervalMs = options.pingIntervalMs ?? 15000;
    this.pingTimeoutMs = options.pingTimeoutMs ?? 10000;
    this.initialReconnectDelayMs = options.initialReconnectDelayMs ?? 250;
    this.maxReconnectDelayMs = options.maxReconnectDelayMs ?? 8000;

    if (options.autoConnect !== false && typeof window !== "undefined") {
      this.connect();
    }
  }

  public getUrl(): string {
    return this.url;
  }

  public setUrl(newUrl: string): void {
    const normalized = normalizeWsUrl(newUrl);
    if (this.url === normalized) return;
    this.url = normalized;
    if (typeof window !== "undefined" && window.localStorage) {
      try {
        window.localStorage.setItem(STORAGE_KEY_WS_URL, normalized);
      } catch {
        // ignore
      }
    }
    if (this.state === "connected" || this.state === "connecting" || this.state === "reconnecting") {
      this.reconnectImmediate();
    }
  }

  public getState(): ConnectionState {
    return this.state;
  }

  public getMeta(): ConnectionMeta {
    return {
      state: this.state,
      url: this.url,
      latencyMs: this.latencyMs,
      lastPingAt: this.lastPingAt,
      lastSeq: this.lastSeq,
      reconnectAttempts: this.reconnectAttempts,
      error: this.lastError,
    };
  }

  public getLastSeq(): number {
    return this.lastSeq;
  }

  public setLastSeq(seq: number): void {
    if (seq > this.lastSeq) {
      this.lastSeq = seq;
    }
  }

  public getLatency(): number | null {
    return this.latencyMs;
  }

  public onStateChange(listener: (state: ConnectionState, meta: ConnectionMeta) => void): () => void {
    this.stateListeners.add(listener);
    listener(this.state, this.getMeta());
    return () => {
      this.stateListeners.delete(listener);
    };
  }

  private notifyStateChange(newState: ConnectionState, error: string | null = null): void {
    this.state = newState;
    if (error !== null) {
      this.lastError = error;
    }
    const meta = this.getMeta();
    this.stateListeners.forEach((listener) => {
      try {
        listener(newState, meta);
      } catch (err) {
        console.error("State listener error:", err);
      }
    });
  }

  public connect(): void {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    if (!this.WebSocketClass) {
      this.notifyStateChange("disconnected", "WebSocket is not available");
      return;
    }

    this.isExplicitlyClosed = false;
    this.clearReconnectTimer();
    this.notifyStateChange(this.reconnectAttempts > 0 ? "reconnecting" : "connecting");

    try {
      this.ws = new this.WebSocketClass(this.url);
    } catch (err: any) {
      this.lastError = err?.message || String(err);
      this.scheduleReconnect();
      return;
    }

    this.ws.onopen = this.handleOpen.bind(this);
    this.ws.onmessage = this.handleMessage.bind(this);
    this.ws.onerror = this.handleError.bind(this);
    this.ws.onclose = this.handleClose.bind(this);
  }

  public disconnect(): void {
    this.isExplicitlyClosed = true;
    this.clearReconnectTimer();
    this.stopHeartbeat();
    this.rejectAllPending("Client disconnected");

    if (this.ws) {
      // Remove handlers before closing
      this.ws.onopen = null as any;
      this.ws.onmessage = null as any;
      this.ws.onerror = null as any;
      this.ws.onclose = null as any;
      try {
        this.ws.close(1000, "Client disconnect");
      } catch {
        // ignore
      }
      this.ws = null;
    }

    this.activeSubscriptionId = null;
    this.notifyStateChange("disconnected");
  }

  private reconnectImmediate(): void {
    this.disconnect();
    this.isExplicitlyClosed = false;
    this.reconnectAttempts = 0;
    this.connect();
  }

  private handleOpen(): void {
    this.reconnectAttempts = 0;
    this.lastError = null;
    this.notifyStateChange("connected");
    this.startHeartbeat();

    // Catch up gaplessly: automatically resubscribe from lastSeq
    if (this.subscribers.size > 0 || this.lastSeq > 0) {
      this.resubscribeFromLastSeq();
    }
  }

  private handleMessage(event: MessageEvent): void {
    let data: any;
    try {
      data = typeof event.data === "string" ? JSON.parse(event.data) : event.data;
    } catch (err) {
      console.warn("Received malformed WS message frame:", event.data);
      return;
    }

    // 1. Check if it's a notification frame
    if (data && typeof data === "object" && "notification" in data) {
      this.handleNotification(data as DaemonNotification);
      return;
    }

    // 2. Check if it's a request/response frame
    if (data && typeof data === "object" && "id" in data) {
      const response = data as DaemonResponse;
      const pending = this.pendingRequests.get(response.id as any);
      if (pending) {
        clearTimeout(pending.timeoutId);
        this.pendingRequests.delete(response.id as any);

        if (response.ok) {
          pending.resolve(response.result);
        } else {
          const err = new Error(response.error?.message || `RPC error: ${response.error?.code}`);
          (err as any).code = response.error?.code;
          pending.reject(err);
        }
      }
    }
  }

  private handleNotification(notif: DaemonNotification): void {
    if (notif.notification === "event") {
      const eventWire = notif.event;
      if (eventWire) {
        if (typeof eventWire.seq === "number" && eventWire.seq > this.lastSeq) {
          this.lastSeq = eventWire.seq;
        }
        // Dispatch to all subscribers
        this.subscribers.forEach((cb) => {
          try {
            cb(eventWire);
          } catch (err) {
            console.error("Error in event subscriber callback:", err);
          }
        });
      }
    } else if (notif.notification === "subscription.closed") {
      if (this.activeSubscriptionId === notif.subscriptionId) {
        this.activeSubscriptionId = null;
      }
      // If server closed due to slow consumer or error, resubscribe immediately from lastSeq
      if (notif.reason === "slow_consumer" && !this.isExplicitlyClosed && this.state === "connected") {
        this.resubscribeFromLastSeq();
      }
    } else if (notif.notification === "daemon.stopping") {
      this.notifyStateChange("disconnected", "Daemon stopping");
    }
  }

  private handleError(_event: Event): void {
    this.lastError = "WebSocket connection error";
  }

  private handleClose(event: CloseEvent): void {
    this.stopHeartbeat();
    this.rejectAllPending(`WebSocket closed (code ${event.code}: ${event.reason || "no reason"})`);
    this.ws = null;
    this.activeSubscriptionId = null;

    if (!this.isExplicitlyClosed) {
      this.notifyStateChange("disconnected", `Connection lost (${event.code})`);
      this.scheduleReconnect();
    } else {
      this.notifyStateChange("disconnected");
    }
  }

  private scheduleReconnect(): void {
    if (this.isExplicitlyClosed) return;
    this.clearReconnectTimer();

    // Exponential backoff with jitter: 250ms -> 8s cap
    const baseDelay = Math.min(
      this.maxReconnectDelayMs,
      this.initialReconnectDelayMs * Math.pow(1.6, this.reconnectAttempts)
    );
    const jitter = Math.random() * 200;
    const delay = Math.min(this.maxReconnectDelayMs, baseDelay + jitter);

    this.reconnectAttempts++;
    this.notifyStateChange("reconnecting");

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.pingTimer = setInterval(() => {
      this.sendPing();
    }, this.pingIntervalMs);
  }

  private stopHeartbeat(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    if (this.pingTimeoutTimer) {
      clearTimeout(this.pingTimeoutTimer);
      this.pingTimeoutTimer = null;
    }
  }

  private sendPing(): void {
    if (this.state !== "connected" || !this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }

    const start = Date.now();
    this.lastPingAt = new Date().toISOString();

    // Set ping timeout guard (10s)
    if (this.pingTimeoutTimer) clearTimeout(this.pingTimeoutTimer);
    this.pingTimeoutTimer = setTimeout(() => {
      // Heartbeat dead: terminate socket and force reconnect
      console.warn("WS ping timed out after 10s, forcing reconnect");
      if (this.ws) {
        try {
          this.ws.close(4000, "Ping timeout");
        } catch {
          // ignore
        }
      }
    }, this.pingTimeoutMs);

    this.call("ping", {})
      .then((_res: any) => {
        if (this.pingTimeoutTimer) {
          clearTimeout(this.pingTimeoutTimer);
          this.pingTimeoutTimer = null;
        }
        this.latencyMs = Date.now() - start;
        this.notifyStateChange("connected");
      })
      .catch((_err) => {
        // If ping rejected or socket died
        if (this.pingTimeoutTimer) {
          clearTimeout(this.pingTimeoutTimer);
          this.pingTimeoutTimer = null;
        }
      });
  }

  /**
   * Request/response correlation by id (promise-based call)
   */
  public call<T = any>(method: string, params: any = {}, timeoutMs = 10000): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        return reject(new Error(`Cannot call ${method}: WebSocket is not connected (state: ${this.state})`));
      }

      const id = this.nextRequestId++;
      const req: DaemonRequest = { id, method, params };

      const timeoutId = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error(`Request timed out after ${timeoutMs}ms for method ${method} (id ${id})`));
        }
      }, timeoutMs);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeoutId,
        method,
      });

      try {
        this.ws.send(JSON.stringify(req));
      } catch (err) {
        clearTimeout(timeoutId);
        this.pendingRequests.delete(id);
        reject(err);
      }
    });
  }

  /**
   * Subscribe to journal event stream starting after a sequence number.
   * Gapless replay + live tail per contract §3.2.
   */
  public subscribe(afterSeq = 0, onEvent: (event: EventWire) => void): () => void {
    this.subscribers.add(onEvent);

    if (this.state === "connected" && !this.activeSubscriptionId) {
      this.resubscribeFromLastSeq(afterSeq);
    }

    return () => {
      this.subscribers.delete(onEvent);
      if (this.subscribers.size === 0 && this.activeSubscriptionId && this.state === "connected") {
        const subId = this.activeSubscriptionId;
        this.activeSubscriptionId = null;
        this.call("events.unsubscribe", { subscriptionId: subId }).catch(() => {
          // ignore unsubscribe errors during teardown
        });
      }
    };
  }

  private async resubscribeFromLastSeq(afterSeqOverride?: number): Promise<void> {
    if (this.state !== "connected" || !this.ws) return;

    const afterSeq = afterSeqOverride !== undefined ? afterSeqOverride : this.lastSeq;
    try {
      const res = await this.call<{ subscriptionId: string }>("events.subscribe", { afterSeq });
      if (res && res.subscriptionId) {
        this.activeSubscriptionId = res.subscriptionId;
      }
    } catch (err) {
      console.warn("Failed to subscribe/resubscribe to events:", err);
    }
  }

  private rejectAllPending(reason: string): void {
    this.pendingRequests.forEach(({ reject, timeoutId, method }) => {
      clearTimeout(timeoutId);
      reject(new Error(`${reason} (method: ${method})`));
    });
    this.pendingRequests.clear();
  }
}

// Global singleton client for the application
let globalWsClient: DaemonWsClient | null = null;

export function getDaemonWsClient(): DaemonWsClient {
  if (!globalWsClient) {
    globalWsClient = new DaemonWsClient({ autoConnect: true });
  }
  return globalWsClient;
}
