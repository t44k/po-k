//! CC lifecycle hook ingestion. Maps CC's hook event name to a po-k event
//! kind (with payload-aware remaps for idle_prompt and permission_prompt
//! notifications), appends it, and wakes long-poll/SSE waiters.

use serde_json::{json, Value};

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

/// Map a CC hook event name to po-k's canonical event kind.
pub fn hook_kind(event: &str) -> String {
    match event {
        "UserPromptSubmit" => "user_prompt".to_string(),
        "Stop" => "stop".to_string(),
        "SubagentStop" => "subagent_stop".to_string(),
        "PostToolUse" => "tool_result".to_string(),
        "Notification" => "notification".to_string(),
        "SessionEnd" => "session_end".to_string(),
        other => format!("hook_{other}"),
    }
}

/// Payload-aware remap applied after [`hook_kind`]. CC fires a Notification
/// hook with `notification_type: "idle_prompt"` ("Claude is waiting for your
/// input") after every completed turn — semantically idle, not a request for
/// intervention. Stored as `notification` it would make every finished turn
/// derive as `awaiting_input`, so it's remapped to `idle_notification`, a kind
/// `latest_status_seqs` doesn't select. All other notifications (e.g.
/// permission prompts) keep `notification` and still drive `awaiting_input`.
fn remap_kind(kind: String, payload: &Value) -> String {
    if kind == "notification" {
        match payload.get("notification_type").and_then(Value::as_str) {
            Some("idle_prompt") => return "idle_notification".to_string(),
            // CC's native TUI permission dialog: drives awaiting_input like a
            // notification, but gets its own kind so consumers can see the
            // picker (pane + options) and answer it with keys.
            Some("permission_prompt") => return "permission_prompt".to_string(),
            _ => {}
        }
    }
    kind
}

/// Parse a CC picker from pane text: numbered options (`❯ 1. Yes`,
/// `  2. Yes, and don't ask again`, `  3. No`) with the highlighted one marked.
pub fn parse_options(pane: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for line in pane.lines() {
        let t = line.trim_start();
        let (selected, rest) = match t.strip_prefix('❯') {
            Some(r) => (true, r.trim_start()),
            None => (false, t),
        };
        let Some((num, label)) = rest.split_once(". ") else { continue };
        if num.is_empty() || num.len() > 2 || !num.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let label = label.trim_end();
        if label.is_empty() {
            continue;
        }
        out.push(json!({ "index": num.parse::<u32>().unwrap_or(0), "label": label, "selected": selected }));
    }
    out
}

const PANE_TAIL_LINES: usize = 40;

/// Attach what the picker looks like, so the deciding event / webhook carries
/// enough for the orchestrator to choose (`keys: ["<index>", "enter"]`).
async fn enrich_permission_prompt(state: &AppState, sid: &str, payload: &mut Value) {
    let Some(running) = state.sessions.get(sid).await else { return };
    let Ok(pane) = crate::zellij::read_focused_pane(&running.zellij_session).await else { return };
    let lines: Vec<&str> = pane.lines().collect();
    let start = lines.len().saturating_sub(PANE_TAIL_LINES);
    let tail = lines[start..].join("\n");
    if let Value::Object(map) = payload {
        map.insert("options".into(), Value::Array(parse_options(&tail)));
        map.insert("pane".into(), Value::String(tail));
    }
}

pub async fn ingest(
    state: &AppState,
    sid: &str,
    event: &str,
    payload: Value,
) -> CoreResult<CoreResponse> {
    if state.sessions.get(sid).await.is_none() {
        return Err(CoreError::not_found(sid));
    }
    let mut payload = payload;
    let kind = remap_kind(hook_kind(event), &payload);
    if kind == "permission_prompt" {
        enrich_permission_prompt(state, sid, &mut payload).await;
    }
    let seq = super::events::record(state, sid, &kind, &payload)
        .await
        .map_err(internal)?;
    Ok(CoreResponse::ok(serde_json::json!({ "ok": true, "seq": seq })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn remap(event: &str, payload: Value) -> String {
        remap_kind(hook_kind(event), &payload)
    }

    #[test]
    fn idle_prompt_notification_remaps_to_idle_notification() {
        let kind = remap(
            "Notification",
            json!({ "notification_type": "idle_prompt", "message": "Claude is waiting for your input" }),
        );
        assert_eq!(kind, "idle_notification");
    }

    #[test]
    fn other_notification_types_keep_notification_kind() {
        let kind = remap(
            "Notification",
            json!({ "notification_type": "permission", "message": "Claude needs your permission" }),
        );
        assert_eq!(kind, "notification");
    }

    #[test]
    fn permission_prompt_notification_gets_its_own_kind() {
        // CC fires Notification with notification_type="permission_prompt" for
        // its native permission dialog. It gets a dedicated kind that still
        // drives awaiting_input, and carries the picker's options.
        let kind = remap(
            "Notification",
            json!({ "notification_type": "permission_prompt", "message": "Allow Bash?" }),
        );
        assert_eq!(kind, "permission_prompt");
    }

    #[test]
    fn parses_picker_options_from_pane_text() {
        let pane = "Do you want to proceed?\n\n ❯ 1. Yes\n   2. Yes, and don't ask again for this command\n   3. No, and tell Claude what to do differently (esc)\n\n Enter to confirm · Esc to cancel";
        let opts = parse_options(pane);
        assert_eq!(opts.len(), 3);
        assert_eq!(opts[0], json!({ "index": 1, "label": "Yes", "selected": true }));
        assert_eq!(opts[1]["index"], 2);
        assert_eq!(opts[1]["selected"], false);
        assert!(opts[2]["label"].as_str().unwrap().starts_with("No, and tell"));
        assert!(parse_options("❯ Try \"write a test\"\n2026. not an option").is_empty());
    }

    #[test]
    fn notification_without_type_keeps_notification_kind() {
        assert_eq!(remap("Notification", json!({ "message": "hi" })), "notification");
        assert_eq!(remap("Notification", json!({})), "notification");
        // notification_type present but not a string — no remap.
        assert_eq!(
            remap("Notification", json!({ "notification_type": 7 })),
            "notification"
        );
    }

    #[test]
    fn non_notification_events_never_remap() {
        // Even with an idle_prompt-shaped payload, only Notification remaps.
        let payload = json!({ "notification_type": "idle_prompt" });
        assert_eq!(remap_kind(hook_kind("Stop"), &payload), "stop");
        assert_eq!(remap_kind(hook_kind("UserPromptSubmit"), &payload), "user_prompt");
    }
}
