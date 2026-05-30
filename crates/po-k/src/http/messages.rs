//! POST /sessions/:id/messages — write text into the pane (with trailing \r).
//! POST /sessions/:id/interrupt — write ESC.
//! POST /sessions/:id/clear     — write `/clear\r`.
//! POST /sessions/:id/files     — drop a base64 file into <cwd>/.po-k-inbox/.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

use crate::state::AppState;
use crate::zellij;

/// How long to wait for CC's `❯` prompt before giving up on a write. Generous
/// because a cold opus boot — or finishing a long in-flight turn — can take a
/// while, and silently dropping the input is worse than blocking.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Deserialize)]
pub struct MessageBody {
    pub text: String,
}

pub async fn message(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<MessageBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let zs = require_session(&state, &sid).await?.zellij_session;
    // CC silently drops input typed before its REPL is ready, and it only
    // writes the transcript JSONL after the first *submitted* prompt — so block
    // until the ❯ prompt is on screen before sending anything.
    zellij::wait_for_cc_prompt(&zs, READY_TIMEOUT).await.map_err(zellij_err)?;
    // Capture the cursor BEFORE writing: CC records this turn's `user_prompt`
    // asynchronously (via the JSONL tailer) only after the pane receives the
    // text, so this seq cleanly precedes the new turn. The orchestrator passes
    // it to `GET /wait?since=<cursor>` for a race-free "block until CC stops".
    let cursor = crate::events_store::current_cursor(&state.db, &sid)
        .await
        .map_err(crate::http::events::internal)?
        .unwrap_or(0);
    zellij::submit_text(&zs, &body.text).await.map_err(zellij_err)?;
    Ok(Json(json!({ "ok": true, "bytes": body.text.len(), "cursor": cursor })))
}

pub async fn interrupt(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let zs = require_session(&state, &sid).await?.zellij_session;
    zellij::send_escape(&zs).await.map_err(zellij_err)?;
    Ok(Json(json!({ "ok": true })))
}

pub async fn clear(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let zs = require_session(&state, &sid).await?.zellij_session;
    zellij::wait_for_cc_prompt(&zs, READY_TIMEOUT).await.map_err(zellij_err)?;
    zellij::submit_text(&zs, "/clear").await.map_err(zellij_err)?;
    Ok(Json(json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
pub struct FileBody {
    pub filename: String,
    pub content_base64: String,
}

pub async fn upload_file(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<FileBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = require_session(&state, &sid).await?;
    if body.filename.is_empty()
        || body.filename.contains('/')
        || body.filename.contains('\\')
        || body.filename.contains("..")
    {
        return Err(bad_request("filename must be a bare name (no slashes or ..)"));
    }
    let bytes = STANDARD
        .decode(body.content_base64.as_bytes())
        .map_err(|e| bad_request(&format!("base64 decode failed: {e}")))?;
    let inbox = PathBuf::from(&session.cwd).join(".po-k-inbox");
    std::fs::create_dir_all(&inbox).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("create_dir_all: {e}") })),
        )
    })?;
    let target = inbox.join(&body.filename);
    std::fs::write(&target, &bytes).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("write: {e}") })),
        )
    })?;
    Ok(Json(json!({
        "ok": true,
        "path": target.to_string_lossy(),
        "bytes": bytes.len(),
    })))
}

async fn require_session(
    state: &AppState,
    sid: &str,
) -> Result<crate::session::RunningSession, (StatusCode, Json<Value>)> {
    state.sessions.get(sid).await.ok_or((
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("session {sid} not found") })),
    ))
}

fn zellij_err(e: anyhow::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("zellij: {e}") })),
    )
}

fn bad_request(msg: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": msg })),
    )
}
