//! `POST /sessions`, `GET /sessions[/:id]`, `DELETE /sessions/:id`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::session::{self, RunningSession, SpawnError};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub project: String,
}

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    match session::spawn(&state, &body.project).await {
        Ok(s) => Ok((StatusCode::CREATED, Json(view_full(&s)))),
        Err(SpawnError::UnknownProject(name)) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("unknown project {name:?}") })),
        )),
        Err(SpawnError::AlreadyRunning { project, sid }) => Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": format!("a session for project {project:?} is already running"),
                "session_id": sid,
            })),
        )),
        Err(SpawnError::Other(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("{e:#}") })),
        )),
    }
}

pub async fn list(State(state): State<AppState>) -> Json<Vec<Value>> {
    let sessions = state.sessions.list().await;
    Json(sessions.iter().map(view_full).collect())
}

pub async fn detail(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .sessions
        .get(&sid)
        .await
        .map(|s| Json(view_full(&s)))
        .ok_or((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("session {sid} not found") })),
        ))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if state.sessions.get(&sid).await.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("session {sid} not found") })),
        ));
    }
    session::kill(&state, &sid).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("{e}") })),
        )
    })?;
    Ok(Json(json!({ "ok": true, "session_id": sid })))
}

fn view_full(s: &RunningSession) -> Value {
    json!({
        "session_id": s.sid,
        "project": s.project,
        "cwd": s.cwd,
        "zellij_session": s.zellij_session,
        "model": s.model,
        "effort": s.effort,
        "started_at": s.started_at,
        "pid": s.pid,
        "hooks_path": s.hooks_path,
        "mcp_path": s.mcp_path,
    })
}
