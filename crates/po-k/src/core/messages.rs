//! Message input: submit text, interrupt, clear, file upload.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::state::AppState;
use crate::zellij;

/// How long to wait for CC's `❯` prompt before giving up on a write.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

async fn require_session(
    state: &AppState,
    sid: &str,
) -> CoreResult<crate::session::RunningSession> {
    state
        .sessions
        .get(sid)
        .await
        .ok_or_else(|| CoreError::not_found(sid))
}

pub async fn send(state: &AppState, sid: &str, text: &str) -> CoreResult<CoreResponse> {
    let zs = require_session(state, sid).await?.zellij_session;
    zellij::wait_for_cc_prompt(&zs, READY_TIMEOUT)
        .await
        .map_err(internal)?;
    // Capture the cursor BEFORE writing so the orchestrator can `wait?since=`
    // race-free.
    let cursor = crate::events_store::current_cursor(&state.db, sid)
        .await
        .map_err(internal)?
        .unwrap_or(0);
    zellij::submit_text(&zs, text).await.map_err(internal)?;
    Ok(CoreResponse::ok(
        json!({ "ok": true, "bytes": text.len(), "cursor": cursor }),
    ))
}

pub async fn interrupt(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    let zs = require_session(state, sid).await?.zellij_session;
    zellij::send_escape(&zs).await.map_err(internal)?;
    Ok(CoreResponse::ok(json!({ "ok": true })))
}

pub async fn clear(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    let zs = require_session(state, sid).await?.zellij_session;
    zellij::wait_for_cc_prompt(&zs, READY_TIMEOUT)
        .await
        .map_err(internal)?;
    zellij::submit_text(&zs, "/clear").await.map_err(internal)?;
    Ok(CoreResponse::ok(json!({ "ok": true })))
}

pub async fn upload_file(
    state: &AppState,
    sid: &str,
    filename: &str,
    content_base64: &str,
) -> CoreResult<CoreResponse> {
    let session = require_session(state, sid).await?;
    if filename.is_empty()
        || filename.contains('/')
        || filename.contains('\\')
        || filename.contains("..")
    {
        return Err(CoreError::BadRequest(
            "filename must be a bare name (no slashes or ..)".into(),
        ));
    }
    let bytes = STANDARD
        .decode(content_base64.as_bytes())
        .map_err(|e| CoreError::BadRequest(format!("base64 decode failed: {e}")))?;
    let inbox = PathBuf::from(&session.cwd).join(".po-k-inbox");
    std::fs::create_dir_all(&inbox).map_err(internal)?;
    let target = inbox.join(filename);
    std::fs::write(&target, &bytes).map_err(internal)?;
    Ok(CoreResponse::ok(json!({
        "ok": true,
        "path": target.to_string_lossy(),
        "bytes": bytes.len(),
    })))
}
