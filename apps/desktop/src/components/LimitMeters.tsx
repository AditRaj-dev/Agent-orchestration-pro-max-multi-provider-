// Provider rate-limit chips for the top bar.
//
// Fed by the daemon's `usage.limits` RPC (crates/agentos-daemon/src/
// usage_meters.rs), which is a cheap local file read — so this polls
// rather than subscribing to an event stream.
//
// The three providers report at different fidelities and the chips show
// that honestly: Codex and Claude render a real percentage bar, while
// Antigravity — which publishes no quota state anywhere on disk — renders
// a status dot and its reset countdown, and nothing more.
import { useEffect, useState } from "react";
import { getDaemonWsClient } from "../daemon/ws";

const POLL_MS = 30_000;

export type MeterState = "ok" | "warning" | "exhausted" | "unknown";

export interface Meter {
  provider: string;
  label: string;
  usedFraction: number | null;
  state: MeterState;
  resetsAt: number | null;
  observedAt: number;
  detail: string | null;
}

const STATE_COLOR: Record<MeterState, string> = {
  ok: "var(--status-complete)",
  warning: "var(--status-waiting)",
  exhausted: "var(--status-failed)",
  unknown: "var(--status-idle)",
};

/** `12345` seconds -> `3h25m`; under a minute reads as `<1m`. */
export function formatCountdown(seconds: number): string {
  if (seconds <= 0) return "now";
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d${hours}h`;
  if (hours > 0) return `${hours}h${minutes}m`;
  if (minutes > 0) return `${minutes}m`;
  return "<1m";
}

function tooltip(meter: Meter, nowSecs: number): string {
  const lines = [meter.label];
  if (meter.usedFraction !== null) {
    lines.push(`${Math.round(meter.usedFraction * 100)}% of the window used`);
  }
  if (meter.state === "exhausted") lines.push("limit reached");
  if (meter.resetsAt !== null) {
    lines.push(`resets in ${formatCountdown(meter.resetsAt - nowSecs)}`);
  }
  if (meter.detail) lines.push(meter.detail);
  const ageMins = Math.floor((nowSecs - meter.observedAt) / 60);
  if (Number.isFinite(ageMins) && ageMins > 0) lines.push(`read ${formatCountdown(ageMins * 60)} ago`);
  return lines.join(" · ");
}

function MeterChip({ meter, nowSecs }: { meter: Meter; nowSecs: number }) {
  const color = STATE_COLOR[meter.state] ?? STATE_COLOR.unknown;
  const percent = meter.usedFraction === null ? null : Math.round(meter.usedFraction * 100);
  const countdown = meter.resetsAt === null ? null : formatCountdown(meter.resetsAt - nowSecs);

  return (
    <div className="telemetry-chip limit-chip" title={tooltip(meter, nowSecs)}>
      <span className="limit-chip-label">{meter.label}</span>

      {percent === null ? (
        // No percentage exists for this provider — a dot is the whole
        // truth it can tell.
        <span className="limit-chip-dot" style={{ backgroundColor: color }} />
      ) : (
        <span className="limit-chip-bar">
          <span
            className="limit-chip-fill"
            style={{ width: `${Math.min(100, Math.max(2, percent))}%`, backgroundColor: color }}
          />
        </span>
      )}

      <span className="telemetry-value" style={{ color }}>
        {percent !== null
          ? `${percent}%`
          : meter.state === "exhausted"
            ? "spent"
            : meter.state === "unknown"
              ? "—"
              : "ok"}
      </span>

      {countdown && <span className="limit-chip-reset">{countdown}</span>}
    </div>
  );
}

export function LimitMeters() {
  const [meters, setMeters] = useState<Meter[]>([]);
  const [nowSecs, setNowSecs] = useState(() => Math.floor(Date.now() / 1000));

  useEffect(() => {
    let live = true;
    const refresh = async () => {
      try {
        const result = await getDaemonWsClient().call<{ meters: Meter[] }>("usage.limits", {});
        if (live && result?.meters) setMeters(result.meters);
      } catch {
        // A disconnected daemon already shows in the connection indicator;
        // the chips just keep their last reading.
      }
      if (live) setNowSecs(Math.floor(Date.now() / 1000));
    };
    void refresh();
    const timer = setInterval(() => void refresh(), POLL_MS);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, []);

  if (meters.length === 0) return null;

  return (
    <>
      {meters.map((meter) => (
        <MeterChip key={`${meter.provider}:${meter.label}`} meter={meter} nowSecs={nowSecs} />
      ))}
    </>
  );
}
