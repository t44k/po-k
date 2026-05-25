//! POST /sessions/:id/messages — write text into the pane (with trailing \n).
//! POST /sessions/:id/interrupt — write ESC.
//! POST /sessions/:id/clear     — write `/clear\n`.
//! POST /sessions/:id/files     — drop a base64 file into <cwd>/.po-k-inbox/.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;

use crate::state::AppState;
use crate::zellij;

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
    let payload = format!("{}\n", body.text);
    zellij::write_chars(&zs, &payload).await.map_err(zellij_err)?;
    Ok(Json(json!({ "ok": true, "bytes": payload.len() })))
}

pub async fn interrupt(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let zs = require_session(&state, &sid).await?.zellij_session;
    zellij::write_chars(&zs, "\x1b").await.map_err(zellij_err)?;
    Ok(Json(json!({ "ok": true })))
}

pub async fn clear(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let zs = require_session(&state, &sid).await?.zellij_session;
    zellij::write_chars(&zs, "/clear\n").await.map_err(zellij_err)?;
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
