//! One task per active watch: long-poll the remote session's `/wait` and
//! persist each new turn boundary as a notification (exactly once per watch and
//! boundary). The deliverer, not the watcher, talks to the webhook.
//!
//! Events (`event` in the notification):
//!   `finished`            — status idle (the `stop` hook landed)          [needs ack]
//!   `needs_input`         — status awaiting_input (permission / question) [needs ack]
//!   `ended`               — the session ended; the watch is done          [needs ack]
//!   `session_lost`        — the box no longer knows the session (404)     [needs ack]
//!   `connection_lost`     — 3 consecutive failures reaching the box       [informational]
//!   `connection_restored` — the box answered again                        [informational]
//!   `auth_failed`         — the box rejected the fleet token; watch failed [informational]
//!   `version_mismatch`    — the box runs a different po-k build; failed   [informational]

use serde_json::Value;
use std::time::Duration;

use super::store::{self, WatchRow};
use crate::state::AppState;

/// Remote `/wait` timeout per iteration (server caps at 600).
pub const WAIT_TIMEOUT_SECS: u64 = 300;
/// Failures in a row before `connection_lost` fires.
pub const LOST_AFTER_FAILURES: u32 = 3;

pub fn event_for_status(status: &str) -> Option<&'static str> {
    match status {
        "idle" => Some("finished"),
        "awaiting_input" => Some("needs_input"),
        "ended" => Some("ended"),
        _ => None,
    }
}

/// Routing keys a receiver may template (`{origin.chat_id}`); every key is
/// always present (empty string when unknown) so a template can never render
/// literally. Values come from the watch's `meta`.
pub const ORIGIN_KEYS: &[&str] = &["platform", "chat_id", "chat_name", "thread_id", "user_id", "user_name", "session_key", "hint"];

pub fn origin_from_meta(meta: &Value) -> Value {
    let mut o = serde_json::Map::new();
    for k in ORIGIN_KEYS {
        let v = meta
            .get(*k)
            .map(|v| match v {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        o.insert((*k).to_string(), Value::String(v));
    }
    Value::Object(o)
}

/// Keep only what a receiver needs from a deciding event: kind, seq, ts and
/// the payload po-k attaches for questions / prompts. Never CC prose.
fn deciding_summary(deciding: &Value) -> Value {
    match deciding {
        Value::Object(o) => serde_json::json!({
            "kind": o.get("kind").cloned().unwrap_or(Value::Null),
            "seq": o.get("seq").cloned().unwrap_or(Value::Null),
            "ts": o.get("ts").cloned().unwrap_or(Value::Null),
            "payload": o.get("payload").cloned().unwrap_or(Value::Null),
        }),
        _ => Value::Null,
    }
}

/// Respawn every active watch from the database (startup).
pub async fn respawn_all(state: &AppState) {
    match store::active_watches(&state.db).await {
        Ok(watches) => {
            let n = watches.len();
            for w in watches {
                spawn(state, w);
            }
            if n > 0 {
                tracing::info!(watches = n, "hub watchers respawned");
            }
        }
        Err(e) => tracing::warn!(error = %e, "cannot load hub watches"),
    }
}

pub fn spawn(state: &AppState, watch: WatchRow) {
    let st = state.clone();
    let id = watch.id.clone();
    let handle = tokio::spawn(async move {
        let wid = watch.id.clone();
        run(st.clone(), watch).await;
        st.hub.forget_task(&wid).await;
    });
    let hub = state.hub.clone();
    tokio::spawn(async move {
        hub.register_task(&id, handle.abort_handle()).await;
    });
}

/// Persist an event for the watch and wake the deliverer. Returns whether a
/// new row was created (false = this boundary was already recorded).
async fn enqueue(state: &AppState, watch: &WatchRow, event: &str, status: Option<&str>, boundary: i64, deciding: &Value, message: Option<&str>) -> bool {
    let origin = origin_from_meta(&watch.meta);
    match store::enqueue(&state.db, watch, event, status, boundary, &deciding_summary(deciding), message, &origin).await {
        Ok(Some(n)) => {
            tracing::info!(notification = %n.id, watch = %watch.id, host = %watch.host, sid = %watch.session_id, event, boundary, "notification recorded");
            state.hub.delivery_wake.notify_waiters();
            true
        }
        Ok(None) => false,
        Err(e) => {
            tracing::error!(watch = %watch.id, event, error = %e, "cannot persist notification");
            let _ = store::record_error(&state.db, &watch.id, &format!("persist {event}: {e:#}")).await;
            false
        }
    }
}

async fn run(state: AppState, watch: WatchRow) {
    let Ok(Some(host)) = store::get_host(&state.db, &watch.host).await else {
        let _ = store::set_state(&state.db, &watch.id, "failed", None, Some("host is not connected")).await;
        return;
    };
    let mut since = watch.since_boundary;
    let mut failures: u32 = 0;
    let mut lost_reported = false;
    tracing::info!(watch = %watch.id, host = %watch.host, sid = %watch.session_id, since, "hub watcher started");

    loop {
        let url = format!(
            "{}/sessions/{}/wait?since={since}&timeout={WAIT_TIMEOUT_SECS}",
            host.base_url, watch.session_id
        );
        let res = crate::version::tag(state.hub.client.get(&url))
            .bearer_auth(state.token.raw())
            .timeout(Duration::from_secs(WAIT_TIMEOUT_SECS + 60))
            .send()
            .await;
        let (status_code, body) = match res {
            Ok(resp) => {
                let code = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                (code, text)
            }
            Err(e) => {
                failures += 1;
                let msg = format!("cannot reach {}: {e}", host.base_url);
                let _ = store::record_error(&state.db, &watch.id, &msg).await;
                let _ = store::touch_host(&state.db, &watch.host, Some(&msg)).await;
                if failures == LOST_AFTER_FAILURES && !lost_reported {
                    lost_reported = true;
                    enqueue(&state, &watch, "connection_lost", None, since, &Value::Null, Some(&msg)).await;
                }
                let backoff = (5u64 << failures.min(4)).min(60);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };

        match status_code {
            404 => {
                enqueue(&state, &watch, "session_lost", None, since, &Value::Null, Some("the box no longer knows this session")).await;
                let _ = store::set_state(&state.db, &watch.id, "done", Some("session_lost"), None).await;
                return;
            }
            409 if crate::version::is_mismatch_body(status_code, &body) => {
                let msg = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_else(|| format!("HTTP 409 from {}: {}", host.base_url, body.chars().take(160).collect::<String>()));
                enqueue(&state, &watch, "version_mismatch", None, since, &Value::Null, Some(&msg)).await;
                let _ = store::set_state(&state.db, &watch.id, "failed", Some("version_mismatch"), Some(&msg)).await;
                let _ = store::touch_host(&state.db, &watch.host, Some(&msg)).await;
                return;
            }
            401 | 403 => {
                let msg = format!("HTTP {status_code} from {}: fleet token rejected", host.base_url);
                enqueue(&state, &watch, "auth_failed", None, since, &Value::Null, Some(&msg)).await;
                let _ = store::set_state(&state.db, &watch.id, "failed", Some("auth_failed"), Some(&msg)).await;
                let _ = store::touch_host(&state.db, &watch.host, Some(&msg)).await;
                return;
            }
            200 => {}
            other => {
                failures += 1;
                let msg = format!("HTTP {other} from {}: {}", host.base_url, body.chars().take(160).collect::<String>());
                let _ = store::record_error(&state.db, &watch.id, &msg).await;
                if failures == LOST_AFTER_FAILURES && !lost_reported {
                    lost_reported = true;
                    enqueue(&state, &watch, "connection_lost", None, since, &Value::Null, Some(&msg)).await;
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        }

        failures = 0;
        let _ = store::touch_host(&state.db, &watch.host, None).await;
        if lost_reported {
            lost_reported = false;
            enqueue(&state, &watch, "connection_restored", None, since, &Value::Null, None).await;
        }
        let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        if v.get("timed_out").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let status = v.get("status").and_then(Value::as_str).unwrap_or("").to_string();
        let boundary = v.get("boundary_cursor").and_then(Value::as_i64).unwrap_or(since);
        let deciding = v.get("deciding_event").cloned().unwrap_or(Value::Null);
        let Some(event) = event_for_status(&status) else {
            // `working` with no timeout flag should not happen; poll again.
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        enqueue(&state, &watch, event, Some(&status), boundary, &deciding, None).await;
        since = boundary.max(since);
        if event == "ended" {
            let _ = store::set_state(&state.db, &watch.id, "done", Some("ended"), None).await;
            return;
        }
        let _ = store::record_progress(&state.db, &watch.id, since, event).await;
        if boundary <= watch.since_boundary && boundary == since {
            // Defensive: a boundary that does not advance would spin; wait a beat.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_to_event_mapping() {
        assert_eq!(event_for_status("idle"), Some("finished"));
        assert_eq!(event_for_status("awaiting_input"), Some("needs_input"));
        assert_eq!(event_for_status("ended"), Some("ended"));
        assert_eq!(event_for_status("working"), None);
    }

    #[test]
    fn origin_and_deciding_summary_are_metadata_only() {
        let origin = origin_from_meta(&json!({ "chat_id": "stream:eng", "thread_id": "t1", "platform": "zulip" }));
        assert_eq!(origin["chat_id"], "stream:eng");
        assert_eq!(origin["thread_id"], "t1");
        for k in ORIGIN_KEYS {
            assert!(origin[k].is_string(), "origin.{k} must always be a string");
        }
        assert_eq!(origin["user_name"], "");
        let d = deciding_summary(&json!({ "kind": "user_question", "seq": 12, "ts": "t", "payload": { "question": "which db?" }, "assistant_text": "SECRET PROSE" }));
        assert_eq!(d["kind"], "user_question");
        assert_eq!(d["payload"]["question"], "which db?");
        assert!(!d.to_string().contains("SECRET PROSE"));
        assert_eq!(deciding_summary(&Value::Null), Value::Null);
    }
}
