//! `POST /sessions/:id/hooks/:event` — ingest a Claude Code hook payload.
//!
//! The hook curl from `hooks.json` posts CC's stdin envelope as the body.
//! We record an event row (`kind = "hook_<event>"`) and return immediately
//! so CC's hook subprocess exits fast.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::events_store::{self};
use crate::state::AppState;

pub async fn ingest(
    State(state): State<AppState>,
    Path((sid, event)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if state.sessions.get(&sid).await.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("session {sid} not found") })),
        ));
    }

    let kind = match event.as_str() {
        "UserPromptSubmit" => "user_prompt".to_string(),
        "Stop" => "stop".to_string(),
        "SubagentStop" => "subagent_stop".to_string(),
        "PostToolUse" => "tool_result".to_string(),
        "Notification" => "notification".to_string(),
        "SessionEnd" => "session_end".to_string(),
        other => format!("hook_{other}"),
    };

    let ts = events_store::now_iso();
    let seq = events_store::append_event(&state.db, &sid, &ts, &kind, &payload)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("{e}") })),
            )
        })?;
    state.bus.notify(&sid).await;
    Ok(Json(json!({ "ok": true, "seq": seq })))
}
