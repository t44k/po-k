//! Periodic heartbeat publisher: walks `~/.claude/sessions/*.json`, derives a
//! live-status row per session, and ships them to `/ingest/heartbeat`.
//!
//! The server's `live_sessions` table is what the transcript header consults to
//! show "live / working / idle" pills and to surface running background tasks.

use anyhow::Result;
use po_k_core::{MachineId, SessionKey};
use po_k_proto::HeartbeatRow;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::warn;

use crate::ship::Shipper;

/// How recent an agent-*.jsonl mtime has to be to count as "active subagent".
const SUBAGENT_FRESH_SECS: u64 = 30;

#[derive(Debug, Deserialize)]
struct SessionsFile {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    pid: Option<i64>,
    #[serde(default, rename = "startedAt")]
    started_at: Option<serde_json::Value>,
    #[serde(default, rename = "updatedAt")]
    updated_at: Option<serde_json::Value>,
    #[serde(default)]
    status: Option<String>,
}

/// Spawn the heartbeat task. Returns when the runtime is cancelled.
pub async fn run(
    shipper: Shipper,
    machine_id: MachineId,
    projects_root: PathBuf,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let sessions_dir = match sessions_dir() {
        Some(p) => p,
        None => {
            warn!("could not resolve ~/.claude/sessions; heartbeat disabled");
            return;
        }
    };
    loop {
        tick.tick().await;
        let rows = match collect_rows(&sessions_dir, &projects_root, &machine_id).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "heartbeat scan failed");
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        if let Err(e) = shipper.ship_heartbeat(&rows).await {
            warn!(error = %e, count = rows.len(), "heartbeat ship failed; will retry");
        }
    }
}

fn sessions_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".claude").join("sessions"))
}

/// Convert `/some/cwd` → `-some-cwd`, matching Claude Code's on-disk directory layout.
fn sanitize_cwd(cwd: &str) -> String {
    cwd.replace('/', "-")
}

async fn collect_rows(
    sessions_dir: &Path,
    projects_root: &Path,
    machine_id: &MachineId,
) -> Result<Vec<HeartbeatRow>> {
    let mut out: Vec<HeartbeatRow> = Vec::new();
    let mut rd = match tokio::fs::read_dir(sessions_dir).await {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        let sf: SessionsFile = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if sf.cwd.is_empty() || sf.session_id.is_empty() {
            continue;
        }
        let sanitized = sanitize_cwd(&sf.cwd);
        let session_key = SessionKey::derive(machine_id, &sanitized, &sf.session_id);
        let active_subagents = count_active_subagents(projects_root, &sanitized, &sf.session_id);
        out.push(HeartbeatRow {
            session_key: session_key.as_str().to_string(),
            status: sf.status.unwrap_or_default(),
            pid: sf.pid,
            started_at: sf.started_at.as_ref().map(json_to_string),
            updated_at: sf.updated_at.as_ref().map(json_to_string),
            active_subagents,
            background_tasks: 0,
        });
    }
    Ok(out)
}

fn count_active_subagents(projects_root: &Path, sanitized_cwd: &str, sid: &str) -> u32 {
    let dir = projects_root.join(sanitized_cwd).join(sid).join("subagents");
    let Ok(rd) = std::fs::read_dir(&dir) else { return 0 };
    let now = SystemTime::now();
    let threshold = Duration::from_secs(SUBAGENT_FRESH_SECS);
    let mut n = 0u32;
    for entry in rd.flatten() {
        let p = entry.path();
        let Some(name) = p.file_name().and_then(|s| s.to_str()) else { continue };
        if !(name.starts_with("agent-") && name.ends_with(".jsonl")) {
            continue;
        }
        let Ok(md) = entry.metadata() else { continue };
        let Ok(mtime) = md.modified() else { continue };
        if now.duration_since(mtime).map(|d| d <= threshold).unwrap_or(false) {
            n += 1;
        }
    }
    n
}

fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}
