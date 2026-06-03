//! CC lifecycle hook ingestion. Maps CC's hook event name to a po-k event
//! kind, appends it, and wakes long-poll/SSE waiters.

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

pub async fn ingest(
    state: &AppState,
    sid: &str,
    event: &str,
    payload: Value,
) -> CoreResult<CoreResponse> {
    if state.sessions.get(sid).await.is_none() {
        return Err(CoreError::not_found(sid));
    }
    let kind = hook_kind(event);
    let seq = super::events::record(state, sid, &kind, &payload)
        .await
        .map_err(internal)?;
    Ok(CoreResponse::ok(serde_json::json!({ "ok": true, "seq": seq })))
}
