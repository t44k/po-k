//! CC lifecycle hook ingestion. Maps CC's hook event name to a po-k event
//! kind (with a payload-aware remap for idle_prompt notifications), appends
//! it, and wakes long-poll/SSE waiters.

use serde_json::Value;

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
    if kind == "notification"
        && payload.get("notification_type").and_then(Value::as_str) == Some("idle_prompt")
    {
        return "idle_notification".to_string();
    }
    kind
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
    let kind = remap_kind(hook_kind(event), &payload);
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
