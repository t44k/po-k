//! Per-session JSONL transcript tailer.
//!
//! CC writes its full conversation to `~/.claude/projects/<sanitized-cwd>/<sid>.jsonl`
//! where `<sanitized-cwd>` replaces `/` and `.` with `-` (matches CC's own
//! convention as observed on disk).
//!
//! On `POST /sessions`, we spawn one of these tailers per session. It waits
//! for the file to appear, opens it, reads new bytes as CC appends, parses
//! each line as JSON, and projects each into a typed event in the `events`
//! table.
//!
//! **Timing:** CC does *not* create the transcript at launch — it appears only
//! ~immediately after the first *submitted* prompt. A freshly spawned session
//! can therefore sit idle (no transcript on disk) for as long as the
//! orchestrator takes to send its first message. So we wait for the file for
//! as long as the session is alive rather than against a fixed deadline; the
//! tailer exits cleanly if the session is killed before it ever sends input.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom};

use crate::event_bus::EventBus;
use crate::events_store::{self, Db};
use crate::session::Registry;

const POLL_INTERVAL: Duration = Duration::from_millis(500);

pub fn spawn(db: Db, bus: EventBus, sessions: Registry, sid: String, cwd: String) {
    tokio::spawn(async move {
        if let Err(e) = run(db, bus, sessions, sid.clone(), cwd).await {
            tracing::warn!(sid, error = %e, "jsonl tailer exited");
        }
    });
}

async fn run(db: Db, bus: EventBus, sessions: Registry, sid: String, cwd: String) -> Result<()> {
    let path = transcript_path(&cwd, &sid)?;
    let path = match wait_for_file(&path, &sessions, &sid).await {
        Some(p) => p,
        None => {
            // Session was killed before it ever submitted a prompt, so no
            // transcript was created. Normal lifecycle, not a failure.
            tracing::info!(sid, "session ended before a transcript appeared");
            return Ok(());
        }
    };
    tracing::info!(sid, path = %path.display(), "jsonl tailer attached");

    let file = File::open(&path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    // Resume past lines already ingested in a previous po-k run. The offset
    // is bumped atomically with each append in `append_jsonl_event`, so a
    // crash between the two can't leave us re-ingesting. If the file shrank
    // since we last tailed (shouldn't happen for CC — it never truncates),
    // fall back to the current end of file rather than EBADF on the seek.
    let file_size = file
        .metadata()
        .await
        .ok()
        .map(|m| m.len())
        .unwrap_or(0);
    let stored = events_store::get_jsonl_offset(&db, &sid).await.unwrap_or(0) as u64;
    let mut offset: u64 = if stored > file_size { file_size } else { stored };
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(offset)).await.ok();

    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF — if the session is gone, stop; otherwise sleep and
                // check for newly appended bytes.
                if sessions.get(&sid).await.is_none() {
                    tracing::info!(sid, "session ended; jsonl tailer stopping");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(n) => {
                let next_offset = offset + n as u64;
                let trimmed = line.trim_end();
                if !trimmed.is_empty() {
                    if let Some((kind, payload)) = project_event(trimmed) {
                        let ts = events_store::now_iso();
                        if events_store::append_jsonl_event(
                            &db,
                            &sid,
                            &ts,
                            &kind,
                            &payload,
                            next_offset as i64,
                        )
                        .await
                        .is_ok()
                        {
                            bus.notify(&sid).await;
                        }
                    } else {
                        // Unprojectable line: still advance the offset so we
                        // don't re-read it next restart.
                        let _ = events_store::set_jsonl_offset(&db, &sid, next_offset as i64).await;
                    }
                } else {
                    // Blank line: advance past it too.
                    let _ = events_store::set_jsonl_offset(&db, &sid, next_offset as i64).await;
                }
                offset = next_offset;
            }
            Err(e) => {
                tracing::warn!(sid, error = %e, "jsonl read error; backing off");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}


pub fn transcript_path(cwd: &str, sid: &str) -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("HOME unset"))?;
    let sanitized = sanitize_cwd(cwd);
    Ok(PathBuf::from(home)
        .join(".claude/projects")
        .join(sanitized)
        .join(format!("{sid}.jsonl")))
}

/// CC's per-project dir name. `/workspace` → `-workspace`,
/// `/home/me/with.dot` → `-home-me-with-dot`.
pub fn sanitize_cwd(cwd: &str) -> String {
    cwd.chars()
        .map(|c| if c == '/' || c == '.' { '-' } else { c })
        .collect()
}

/// Wait for CC's transcript to appear. CC creates it only after the first
/// submitted prompt, so there is no useful fixed deadline while the session is
/// alive — we poll until the file shows up, or give up once the session has
/// been removed from the registry (killed before sending anything).
async fn wait_for_file(path: &std::path::Path, sessions: &Registry, sid: &str) -> Option<PathBuf> {
    loop {
        if path.exists() {
            return Some(path.to_path_buf());
        }
        if sessions.get(sid).await.is_none() {
            return None;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Map a raw JSONL line from CC's transcript into a typed event.
///
/// CC's schema (`assistant`, `user`, `tool_use`, `tool_result` etc) is wrapped
/// in an envelope with `type` and a `message.content` array. We surface the
/// common shapes; unknown shapes pass through as `raw`.
fn project_event(line: &str) -> Option<(String, Value)> {
    let v: Value = serde_json::from_str(line).ok()?;
    let ev_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    let message = v.get("message");

    let turn_id = v
        .get("last-prompt")
        .and_then(|p| p.get("leafUuid"))
        .and_then(|u| u.as_str())
        .map(|s| s.to_string());

    match ev_type {
        "user" => {
            // CC writes user prompts AND tool-result envelopes as type=user.
            let content = message.and_then(|m| m.get("content"));
            if let Some(items) = content.and_then(|c| c.as_array()) {
                for item in items {
                    if item.get("type").and_then(|x| x.as_str()) == Some("tool_result") {
                        return Some((
                            "tool_result".to_string(),
                            json!({
                                "tool_use_id": item.get("tool_use_id"),
                                "content": item.get("content"),
                                "is_error": item.get("is_error"),
                                "turn_id": turn_id,
                            }),
                        ));
                    }
                }
            }
            // Plain user text prompt.
            let text = content
                .and_then(|c| c.as_str())
                .or_else(|| {
                    content
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.iter().find_map(|x| x.get("text").and_then(|t| t.as_str())))
                })
                .unwrap_or("")
                .to_string();
            Some(("user_prompt".to_string(), json!({ "text": text, "turn_id": turn_id })))
        }
        "assistant" => {
            // assistant content can be: text, tool_use, thinking. Emit one
            // event per item so the orchestrator sees the structure.
            let content = message.and_then(|m| m.get("content")).and_then(|c| c.as_array());
            let mut texts = Vec::new();
            let mut tool_uses = Vec::new();
            if let Some(items) = content {
                for item in items {
                    match item.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(s) = item.get("text").and_then(|t| t.as_str()) {
                                texts.push(s.to_string());
                            }
                        }
                        Some("tool_use") => {
                            tool_uses.push(json!({
                                "id": item.get("id"),
                                "name": item.get("name"),
                                "input": item.get("input"),
                                "turn_id": turn_id,
                            }));
                        }
                        _ => {}
                    }
                }
            }
            // Pull stop_reason if present on the message.
            let stop_reason = message
                .and_then(|m| m.get("stop_reason"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            // Concatenate text fragments into one assistant_message event.
            if !texts.is_empty() {
                return Some((
                    "assistant_message".to_string(),
                    json!({
                        "text": texts.join("\n"),
                        "stop_reason": stop_reason,
                        "turn_id": turn_id,
                    }),
                ));
            }
            if let Some(tu) = tool_uses.into_iter().next() {
                return Some(("tool_use".to_string(), tu));
            }
            Some(("raw".to_string(), v))
        }
        "result" => {
            // CC's per-turn cost + token summary.
            Some((
                "turn_end".to_string(),
                json!({
                    "stop_reason": v.get("stop_reason"),
                    "total_cost_usd": v.get("total_cost_usd"),
                    "usage": v.get("usage"),
                    "turn_id": turn_id,
                }),
            ))
        }
        "" => Some(("raw".to_string(), v)),
        other => Some((format!("raw_{other}"), v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_known_paths() {
        assert_eq!(sanitize_cwd("/workspace"), "-workspace");
        assert_eq!(sanitize_cwd("/home/me/dotfiles"), "-home-me-dotfiles");
        assert_eq!(sanitize_cwd("/home/me/with.dot"), "-home-me-with-dot");
    }

    #[test]
    fn project_user_text() {
        let line = r#"{"type":"user","message":{"content":[{"type":"text","text":"hi"}]}}"#;
        let (k, _) = project_event(line).unwrap();
        assert_eq!(k, "user_prompt");
    }

    #[test]
    fn project_tool_result() {
        let line = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":"out","is_error":false}]}}"#;
        let (k, v) = project_event(line).unwrap();
        assert_eq!(k, "tool_result");
        assert_eq!(v["tool_use_id"], "t1");
        assert_eq!(v["content"], "out");
    }

    #[test]
    fn project_assistant_message() {
        let line = r#"{"type":"assistant","message":{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}}"#;
        let (k, v) = project_event(line).unwrap();
        assert_eq!(k, "assistant_message");
        assert_eq!(v["text"], "done");
        assert_eq!(v["stop_reason"], "end_turn");
    }

    #[test]
    fn project_assistant_tool_use() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]}}"#;
        let (k, v) = project_event(line).unwrap();
        assert_eq!(k, "tool_use");
        assert_eq!(v["name"], "Bash");
    }

    #[test]
    fn project_turn_end() {
        let line = r#"{"type":"result","stop_reason":"end_turn","total_cost_usd":0.04,"usage":{"input_tokens":10}}"#;
        let (k, v) = project_event(line).unwrap();
        assert_eq!(k, "turn_end");
        assert_eq!(v["total_cost_usd"], 0.04);
    }
}
