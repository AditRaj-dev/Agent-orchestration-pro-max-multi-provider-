//! Provider rate-limit meters for the desktop top bar.
//!
//! Three surfaces, three very different data qualities — the module is
//! honest about each rather than inventing a number:
//!
//! - **Codex** persists its own limits. Every rollout transcript under
//!   `~/.codex/sessions/**/rollout-*.jsonl` carries a `token_count` event
//!   whose `rate_limits` block reports `used_percent`, `window_minutes`
//!   and `resets_at` straight from the server. Pure file read, exact.
//!
//! - **Claude Code** reports the same quality of signal — `rate_limit_event`
//!   with a real `utilization` fraction — but only on the stream-json
//!   wire; nothing under `~/.claude` persists it (transcripts keep
//!   `quotaLimits` only at `status: "rejected"`). So the adapter event is
//!   captured on the way past and cached here.
//!
//! - **Antigravity** publishes no quota state at all. The only signal is
//!   the 429 (`RESOURCE_EXHAUSTED ... Resets in 4h43m48s.`) surfaced by
//!   the agy adapter and by the IDE's own `ls-main.log`. That yields one
//!   bit plus a reset time — no percentage, and no way to tell the Gemini
//!   pool from the Claude pool, so this stays a single combined meter.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Fraction above which a meter reads as merely warm.
const WARN_AT: f64 = 0.75;
/// Fraction above which a meter reads as effectively spent.
const CRITICAL_AT: f64 = 0.90;
/// Newest rollout files to scan before giving up on a Codex reading.
const CODEX_ROLLOUT_SCAN: usize = 6;
/// Bytes of a log tail worth scanning for the last quota rejection.
const LOG_TAIL_BYTES: u64 = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MeterState {
    Ok,
    Warning,
    Exhausted,
    Unknown,
}

/// One limit window, as the desktop renders it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Meter {
    /// Adapter id this belongs to (`claude-code`, `codex`, `antigravity-agy`).
    pub provider: String,
    /// Short chip label, e.g. `Claude · 7d`.
    pub label: String,
    /// 0.0–1.0 of the window consumed. `None` when the provider does not
    /// report one (Antigravity), which the UI renders as a dot, not a bar.
    pub used_fraction: Option<f64>,
    pub state: MeterState,
    /// Unix seconds at which the window rolls over.
    pub resets_at: Option<i64>,
    /// Unix seconds at which this reading was taken — the UI greys out a
    /// stale meter rather than presenting an old number as current.
    pub observed_at: i64,
    /// Provider text worth surfacing on hover.
    pub detail: Option<String>,
}

fn state_for(fraction: f64) -> MeterState {
    if fraction >= CRITICAL_AT {
        MeterState::Exhausted
    } else if fraction >= WARN_AT {
        MeterState::Warning
    } else {
        MeterState::Ok
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Where captured wire events live between daemon restarts.
/// `AGENTOS_USAGE_CACHE` overrides (tests, and a portable install).
pub fn cache_path() -> PathBuf {
    if let Some(path) = std::env::var_os("AGENTOS_USAGE_CACHE") {
        return PathBuf::from(path);
    }
    home_dir().join(".agentos").join("usage-meters.json")
}

// ---------------------------------------------------------------------
// Window labels
// ---------------------------------------------------------------------

/// `10080 -> "7d"`, `300 -> "5h"`, `45 -> "45m"`.
fn window_label(minutes: i64) -> String {
    if minutes <= 0 {
        return "window".to_owned();
    }
    if minutes % 1440 == 0 {
        format!("{}d", minutes / 1440)
    } else if minutes % 60 == 0 {
        format!("{}h", minutes / 60)
    } else {
        format!("{minutes}m")
    }
}

/// Claude names its windows rather than sizing them.
fn claude_window_label(kind: &str) -> String {
    match kind {
        "five_hour" => "5h".to_owned(),
        "seven_day" => "7d".to_owned(),
        "opus_seven_day" => "opus 7d".to_owned(),
        "" => "limit".to_owned(),
        other => other.replace('_', " "),
    }
}

// ---------------------------------------------------------------------
// Codex — exact, straight off disk
// ---------------------------------------------------------------------

/// Parse one `rate_limits` block into its window meters. `primary` and
/// `secondary` are independent windows; either may be absent.
pub fn parse_codex_rate_limits(block: &Value, observed_at: i64) -> Vec<Meter> {
    let plan = block.get("plan_type").and_then(Value::as_str);
    ["primary", "secondary"]
        .iter()
        .filter_map(|slot| {
            let window = block.get(*slot)?;
            let percent = window.get("used_percent").and_then(Value::as_f64)?;
            let fraction = (percent / 100.0).clamp(0.0, 1.0);
            let minutes = window
                .get("window_minutes")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            // Codex has shipped both an absolute stamp and an offset.
            let resets_at = window.get("resets_at").and_then(Value::as_i64).or_else(|| {
                window
                    .get("resets_in_seconds")
                    .and_then(Value::as_i64)
                    .map(|secs| observed_at + secs)
            });
            Some(Meter {
                provider: "codex".to_owned(),
                label: format!("Codex · {}", window_label(minutes)),
                used_fraction: Some(fraction),
                state: state_for(fraction),
                resets_at,
                observed_at,
                detail: plan.map(|p| format!("plan: {p}")),
            })
        })
        .collect()
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>, keep: &dyn Fn(&str) -> bool) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out, keep);
        } else if path.file_name().and_then(|n| n.to_str()).is_some_and(keep) {
            out.push(path);
        }
    }
}

fn file_mtime(path: &Path) -> Option<i64> {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
}

/// Newest-first rollout transcripts under `~/.codex/sessions`.
fn codex_rollouts(root: &Path, limit: usize) -> Vec<PathBuf> {
    let mut found = Vec::new();
    collect_files(root, &mut found, &|name| {
        name.starts_with("rollout-") && name.ends_with(".jsonl")
    });
    found.sort_by_key(|path| std::cmp::Reverse(file_mtime(path).unwrap_or(0)));
    found.truncate(limit);
    found
}

/// Depth-first search for a key, so a schema that nests `rate_limits`
/// one level deeper does not break the reading (F-00 §4.2: schemas drift).
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if let Some(found) = map.get(key) {
                return Some(found);
            }
            map.values().find_map(|v| find_key(v, key))
        }
        Value::Array(items) => items.iter().find_map(|v| find_key(v, key)),
        _ => None,
    }
}

fn read_codex() -> Vec<Meter> {
    let root = home_dir().join(".codex").join("sessions");
    for path in codex_rollouts(&root, CODEX_ROLLOUT_SCAN) {
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        // The newest reading wins, so scan from the end of the transcript.
        for line in text.lines().rev() {
            if !line.contains("\"rate_limits\"") {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let Some(block) = find_key(&value, "rate_limits") else {
                continue;
            };
            let observed_at = file_mtime(&path).unwrap_or_else(now_secs);
            let meters = parse_codex_rate_limits(block, observed_at);
            if !meters.is_empty() {
                return meters;
            }
        }
    }
    Vec::new()
}

// ---------------------------------------------------------------------
// Claude — captured off the wire
// ---------------------------------------------------------------------

/// Parse a `rate_limit_info` notice. Shape observed on the stream-json
/// wire: `{"rateLimitType":"seven_day","status":"allowed_warning",
/// "utilization":0.85,"resetsAt":1788146711}`. The `quotaLimits` block
/// that lands in transcripts on a 429 is the same shape minus
/// `utilization`, and parses here too.
pub fn parse_claude_notice(notice: &Value, observed_at: i64) -> Option<Meter> {
    let kind = notice
        .get("rateLimitType")
        .and_then(Value::as_str)
        .unwrap_or("");
    let status = notice.get("status").and_then(Value::as_str).unwrap_or("");
    let rejected = status == "rejected";
    let utilization = notice.get("utilization").and_then(Value::as_f64);
    if utilization.is_none() && !rejected && kind.is_empty() {
        return None;
    }
    let fraction = utilization.map(|u| u.clamp(0.0, 1.0));
    let state = if rejected {
        MeterState::Exhausted
    } else {
        match fraction {
            Some(value) => state_for(value),
            // `allowed_warning` without a number still means "close".
            None if status.contains("warning") => MeterState::Warning,
            None => MeterState::Unknown,
        }
    };
    Some(Meter {
        provider: "claude-code".to_owned(),
        label: format!("Claude · {}", claude_window_label(kind)),
        // A rejection is 100% of the window by definition.
        used_fraction: fraction.or(if rejected { Some(1.0) } else { None }),
        state,
        resets_at: notice.get("resetsAt").and_then(Value::as_i64),
        observed_at,
        detail: (!status.is_empty()).then(|| status.to_owned()),
    })
}

// ---------------------------------------------------------------------
// Antigravity — one bit and a countdown
// ---------------------------------------------------------------------

/// Seconds in a `4h43m48s` / `146h41m26s` / `12m` duration. Any component
/// may be absent; `None` if none of them are present.
pub fn parse_duration_secs(text: &str) -> Option<i64> {
    let mut total = 0i64;
    let mut digits = String::new();
    let mut matched = false;
    for ch in text.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        let Ok(value) = digits.parse::<i64>() else {
            digits.clear();
            continue;
        };
        digits.clear();
        match ch {
            'h' => total += value * 3600,
            'm' => total += value * 60,
            's' => total += value,
            _ => continue,
        }
        matched = true;
    }
    matched.then_some(total)
}

/// Pull the reset window out of a provider quota message.
/// Observed: `"Individual quota reached. ... Resets in 4h43m48s."`
pub fn parse_agy_quota_message(text: &str, observed_at: i64) -> Option<Meter> {
    let lowered = text.to_ascii_lowercase();
    if !lowered.contains("quota reached")
        && !lowered.contains("quota exceeded")
        && !lowered.contains("resource_exhausted")
    {
        return None;
    }
    let resets_at = lowered
        .split_once("resets in ")
        .and_then(|(_, rest)| parse_duration_secs(rest.split_whitespace().next()?))
        .map(|secs| observed_at + secs);
    Some(Meter {
        provider: "antigravity-agy".to_owned(),
        label: "Antigravity".to_owned(),
        // No percentage exists anywhere in Antigravity's local state, and
        // the 429 does not name the model pool — so this is deliberately
        // one bit, not a bar. See module docs.
        used_fraction: None,
        state: MeterState::Exhausted,
        resets_at,
        observed_at,
        detail: Some("individual quota reached".to_owned()),
    })
}

/// glog prefix `I0709 15:09:21.116896` -> unix seconds.
fn glog_timestamp(line: &str, year: i32) -> Option<i64> {
    use chrono::{Local, NaiveDate, TimeZone};
    let mut parts = line.split_whitespace();
    let stamp = parts.next()?;
    let clock = parts.next()?;
    let digits: String = stamp.chars().filter(char::is_ascii_digit).collect();
    if digits.len() < 4 {
        return None;
    }
    let month: u32 = digits[digits.len() - 4..digits.len() - 2].parse().ok()?;
    let day: u32 = digits[digits.len() - 2..].parse().ok()?;
    let mut hms = clock.split(':');
    let hour: u32 = hms.next()?.parse().ok()?;
    let minute: u32 = hms.next()?.parse().ok()?;
    let second: u32 = hms.next()?.split('.').next()?.parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, second)?;
    Local
        .from_local_datetime(&naive)
        .single()
        .map(|dt| dt.timestamp())
}

/// Last quota rejection in a glog-formatted tail, with the timestamp the
/// log itself recorded. `year` seeds the `MMDD` stamp, which carries no
/// year of its own.
pub fn scan_agy_log(text: &str, year: i32) -> Option<Meter> {
    for line in text.lines().rev() {
        if !line.contains("Resets in ") {
            continue;
        }
        // Prefer the line's own stamp; fall back to "just now" so a format
        // change degrades to a slightly pessimistic reading, never a panic.
        let observed_at = glog_timestamp(line, year).unwrap_or_else(now_secs);
        if let Some(meter) = parse_agy_quota_message(line, observed_at) {
            return Some(meter);
        }
    }
    None
}

/// Read the tail of a large log without loading the whole thing.
fn read_tail(path: &Path, bytes: u64) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len > bytes {
        file.seek(SeekFrom::Start(len - bytes)).ok()?;
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).ok()?;
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// The IDE's own log, which also captures quota burned outside this app.
fn antigravity_log() -> Option<PathBuf> {
    let roaming = std::env::var_os("APPDATA").map(PathBuf::from)?;
    let logs = roaming.join("Antigravity IDE").join("logs");
    let mut dirs: Vec<PathBuf> = fs::read_dir(&logs)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    // Directory names are sortable timestamps (`20260823T140923`).
    dirs.sort();
    dirs.pop().map(|dir| dir.join("ls-main.log"))
}

fn read_antigravity_log() -> Option<Meter> {
    let path = antigravity_log()?;
    let year = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(|n| n.get(..4))
        .and_then(|y| y.parse::<i32>().ok())?;
    scan_agy_log(&read_tail(&path, LOG_TAIL_BYTES)?, year)
}

// ---------------------------------------------------------------------
// Cache: wire events that nothing else persists
// ---------------------------------------------------------------------

fn load_cache() -> Value {
    fs::read_to_string(cache_path())
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_else(|| Value::Object(Default::default()))
}

fn store_cache(cache: &Value) {
    let path = cache_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let Ok(text) = serde_json::to_string_pretty(cache) else {
        return;
    };
    // ponytail: write-then-rename; a torn cache file is recoverable noise,
    // not data loss, so no lock file.
    let tmp = path.with_extension("json.tmp");
    if fs::write(&tmp, text).is_ok() {
        let _ = fs::rename(&tmp, &path);
    }
}

fn cache_put(meter: &Meter) {
    let key = format!("{}::{}", meter.provider, meter.label);
    let mut cache = load_cache();
    if let Some(map) = cache.as_object_mut() {
        if let Ok(value) = serde_json::to_value(meter) {
            map.insert(key, value);
            store_cache(&cache);
        }
    }
}

fn cache_get(key: &str) -> Option<Meter> {
    serde_json::from_value(load_cache().get(key)?.clone()).ok()
}

/// Capture a `rate_limit_event` notice on its way past the session pump.
/// This is the only place Claude's real utilization is ever observable.
pub fn record_rate_limit(adapter_id: &str, notice: &Value) {
    let observed_at = now_secs();
    let meter = match adapter_id {
        "claude-code" => parse_claude_notice(notice, observed_at),
        "antigravity-agy" => notice
            .as_str()
            .or_else(|| notice.get("error").and_then(Value::as_str))
            .and_then(|text| parse_agy_quota_message(text, observed_at)),
        _ => None,
    };
    if let Some(meter) = meter {
        cache_put(&meter);
    }
}

/// Capture an adapter failure that turned out to be a quota window — the
/// agy adapter reports exhaustion as a `Transient` failure, not a
/// `RateLimit` event, so that path needs its own hook.
pub fn record_failure_detail(adapter_id: &str, detail: &str) {
    if adapter_id != "antigravity-agy" {
        return;
    }
    if let Some(meter) = parse_agy_quota_message(detail, now_secs()) {
        cache_put(&meter);
    }
}

// ---------------------------------------------------------------------
// The RPC surface
// ---------------------------------------------------------------------

/// A spent window that has since rolled over is no longer spent.
fn expire(mut meter: Meter, now: i64) -> Meter {
    if meter.resets_at.is_some_and(|at| at <= now) {
        meter.state = MeterState::Ok;
        meter.used_fraction = meter.used_fraction.map(|_| 0.0);
        meter.resets_at = None;
    }
    meter
}

fn unknown(provider: &str, label: &str, detail: &str) -> Meter {
    Meter {
        provider: provider.to_owned(),
        label: label.to_owned(),
        used_fraction: None,
        state: MeterState::Unknown,
        resets_at: None,
        observed_at: now_secs(),
        detail: Some(detail.to_owned()),
    }
}

/// Every meter the daemon can currently produce, ordered for the top bar.
pub fn meters() -> Vec<Meter> {
    let now = now_secs();
    let mut out = Vec::new();

    // Claude: cache only — nothing on disk carries live utilization.
    let mut claude: Vec<Meter> = load_cache()
        .as_object()
        .map(|map| {
            map.iter()
                .filter(|(key, _)| key.starts_with("claude-code::"))
                .filter_map(|(_, v)| serde_json::from_value::<Meter>(v.clone()).ok())
                .collect()
        })
        .unwrap_or_default();
    claude.sort_by(|a, b| a.label.cmp(&b.label));
    if claude.is_empty() {
        out.push(unknown(
            "claude-code",
            "Claude",
            "no reading yet — run Claude once",
        ));
    } else {
        out.extend(claude.into_iter().map(|m| expire(m, now)));
    }

    // Codex: authoritative, straight off disk.
    let codex = read_codex();
    if codex.is_empty() {
        out.push(unknown("codex", "Codex", "no rollout transcript found"));
    } else {
        out.extend(codex.into_iter().map(|m| expire(m, now)));
    }

    // Antigravity: the IDE log and the adapter cache, newest wins.
    let from_log = read_antigravity_log();
    let from_cache = cache_get("antigravity-agy::Antigravity");
    let antigravity = match (from_log, from_cache) {
        (Some(a), Some(b)) => Some(if a.observed_at >= b.observed_at { a } else { b }),
        (a, b) => a.or(b),
    };
    out.push(match antigravity {
        Some(meter) => {
            let meter = expire(meter, now);
            // Expiry resets a bar to zero; for a one-bit meter it means
            // the window rolled over, i.e. usable again.
            if meter.state == MeterState::Ok {
                Meter {
                    detail: Some("quota window has rolled over".to_owned()),
                    ..meter
                }
            } else {
                meter
            }
        }
        None => Meter {
            provider: "antigravity-agy".to_owned(),
            label: "Antigravity".to_owned(),
            used_fraction: None,
            state: MeterState::Ok,
            resets_at: None,
            observed_at: now,
            detail: Some("no quota rejection seen".to_owned()),
        },
    });

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn codex_block_yields_exact_windows() {
        // Verbatim from ~/.codex/sessions/.../rollout-*.jsonl.
        let block = json!({
            "limit_id": "codex",
            "primary": { "used_percent": 94.0, "window_minutes": 10080, "resets_at": 1788146711 },
            "secondary": null,
            "plan_type": "plus"
        });
        let meters = parse_codex_rate_limits(&block, 1788000000);
        assert_eq!(meters.len(), 1, "secondary is null, so one window");
        assert_eq!(meters[0].label, "Codex · 7d");
        assert_eq!(meters[0].used_fraction, Some(0.94));
        assert_eq!(meters[0].state, MeterState::Exhausted);
        assert_eq!(meters[0].resets_at, Some(1788146711));
        assert_eq!(meters[0].detail.as_deref(), Some("plan: plus"));
    }

    #[test]
    fn codex_secondary_window_is_its_own_meter() {
        let block = json!({
            "primary": { "used_percent": 12.5, "window_minutes": 300, "resets_at": 10 },
            "secondary": { "used_percent": 80.0, "window_minutes": 10080, "resets_in_seconds": 60 }
        });
        let meters = parse_codex_rate_limits(&block, 1000);
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].label, "Codex · 5h");
        assert_eq!(meters[0].state, MeterState::Ok);
        assert_eq!(meters[1].label, "Codex · 7d");
        assert_eq!(meters[1].state, MeterState::Warning);
        // An offset is resolved against the observation time.
        assert_eq!(meters[1].resets_at, Some(1060));
    }

    #[test]
    fn claude_wire_notice_carries_real_utilization() {
        // Verbatim shape asserted by the claude adapter's own stream test.
        let notice = json!({
            "rateLimitType": "seven_day",
            "status": "allowed_warning",
            "utilization": 0.85,
            "resetsAt": 1788146711
        });
        let meter = parse_claude_notice(&notice, 42).expect("a meter");
        assert_eq!(meter.label, "Claude · 7d");
        assert_eq!(meter.used_fraction, Some(0.85));
        assert_eq!(meter.state, MeterState::Warning);
        assert_eq!(meter.resets_at, Some(1788146711));
    }

    #[test]
    fn claude_rejection_reads_as_a_full_window() {
        // Verbatim from a ~/.claude transcript at status "rejected".
        let notice = json!({
            "status": "rejected",
            "resetsAt": 1787338800,
            "rateLimitType": "five_hour"
        });
        let meter = parse_claude_notice(&notice, 42).expect("a meter");
        assert_eq!(meter.label, "Claude · 5h");
        assert_eq!(meter.state, MeterState::Exhausted);
        assert_eq!(meter.used_fraction, Some(1.0));
    }

    #[test]
    fn agy_message_yields_one_bit_and_a_countdown() {
        // Verbatim from Antigravity IDE ls-main.log.
        let text = "RESOURCE_EXHAUSTED (code 429): Individual quota reached. \
                    Please upgrade your subscription to increase your limits. Resets in 4h43m48s.";
        let meter = parse_agy_quota_message(text, 1_000_000).expect("a meter");
        assert_eq!(meter.label, "Antigravity");
        assert_eq!(meter.used_fraction, None, "no percentage exists to report");
        assert_eq!(meter.state, MeterState::Exhausted);
        assert_eq!(meter.resets_at, Some(1_000_000 + 4 * 3600 + 43 * 60 + 48));
    }

    #[test]
    fn agy_long_window_parses() {
        let meter =
            parse_agy_quota_message("Individual quota reached. Resets in 146h41m26s.", 0).unwrap();
        assert_eq!(meter.resets_at, Some(146 * 3600 + 41 * 60 + 26));
    }

    #[test]
    fn non_quota_text_is_not_a_meter() {
        assert!(parse_agy_quota_message("user denied permission", 0).is_none());
        assert_eq!(parse_duration_secs("nothing here"), None);
    }

    #[test]
    fn agy_log_scan_takes_the_last_rejection_with_its_own_stamp() {
        let log = "I0709 15:09:21.116896 50700 log.go:398] something ordinary\n\
                   I0709 15:09:21.116896 50700 log.go:398] RESOURCE_EXHAUSTED (code 429): Individual quota reached. Resets in 1h0m0s.\n\
                   I0709 18:09:20.830844 50700 log.go:398] RESOURCE_EXHAUSTED (code 429): Individual quota reached. Resets in 2h0m0s.\n";
        let meter = scan_agy_log(log, 2026).expect("a meter");
        // The later line wins, and its reset is 2h past *its own* stamp.
        let reset = meter.resets_at.expect("a reset");
        assert_eq!(reset - meter.observed_at, 7200);
    }

    #[test]
    fn a_rolled_over_window_is_no_longer_spent() {
        let spent = Meter {
            provider: "codex".to_owned(),
            label: "Codex · 7d".to_owned(),
            used_fraction: Some(0.99),
            state: MeterState::Exhausted,
            resets_at: Some(500),
            observed_at: 400,
            detail: None,
        };
        let fresh = expire(spent, 600);
        assert_eq!(fresh.state, MeterState::Ok);
        assert_eq!(fresh.resets_at, None);
    }

    #[test]
    fn window_labels_read_naturally() {
        assert_eq!(window_label(10080), "7d");
        assert_eq!(window_label(300), "5h");
        assert_eq!(window_label(45), "45m");
        assert_eq!(claude_window_label("five_hour"), "5h");
        assert_eq!(claude_window_label("seven_day"), "7d");
    }
}
