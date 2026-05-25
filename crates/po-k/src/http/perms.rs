//! Permission round-trip endpoints.
//!
//! - `POST /sessions/:id/mcp/approve` (called by `po-k mcp` subprocess CC spawned):
//!   blocks until the orchestrator answers or `cc.permission_timeout` fires;
//!   default on timeout is `deny`.
//! - `POST /sessions/:id/permission_requests/:req_id` (called by orchestrator):
//!   resolves a pending request.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use crate::events_store;
use crate::permissions::Decision;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ApproveBody {
    pub tool_name: String,
    #[serde(default)]
    pub input: Value,
}

pub async fn approve(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<ApproveBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if state.sessions.get(&sid).await.is_none() {
        // Allow approve to work even if the session is in-flight; if the DB
        // doesn't know about it either, then it's a 404.
        if events_store::get_session(&state.db, &sid)
            .await
            .map_err(internal)?
            .is_none()
        {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("session {sid} not found") })),
            ));
        }
    }

    let timeout_ms: u64 = state
        .config
        .read()
        .await
        .cc
        .permission_timeout
        .0
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let request_id = format!("req-{}", Uuid::new_v4().simple());
    let rx = state.perms.register(request_id.clone()).await;

    let payload = json!({
        "request_id": request_id,
        "tool": body.tool_name,
        "input": body.input,
        "timeout_ms": timeout_ms,
    });
    let ts = events_store::now_iso();
    if let Err(e) = events_store::append_event(&state.db, &sid, &ts, "permission_request", &payload)
        .await
    {
        state.perms.forget(&request_id).await;
        return Err(internal(e));
    }
    state.bus.notify(&sid).await;

    let decision = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
        Ok(Ok(d)) => d,
        Ok(Err(_)) => Decision::deny("po-k permission tracker dropped"),
        Err(_) => {
            state.perms.forget(&request_id).await;
            Decision::deny("po-k permission timeout")
        }
    };

    // Record the outcome so the orchestrator's audit log + cost view both see it.
    let outcome = json!({
        "request_id": request_id,
        "behavior": decision.behavior,
        "message": decision.message,
    });
    let _ = events_store::append_event(
        &state.db,
        &sid,
        &events_store::now_iso(),
        "permission_decision",
        &outcome,
    )
    .await;
    state.bus.notify(&sid).await;

    Ok(Json(serde_json::to_value(decision).unwrap_or(json!({}))))
}

#[derive(Debug, Deserialize)]
pub struct ResolveBody {
    pub behavior: String,
    #[serde(default)]
    pub message: Option<String>,
}

pub async fn resolve(
    State(state): State<AppState>,
    Path((sid, req_id)): Path<(String, String)>,
    Json(body): Json<ResolveBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let _ = &sid; // session id is informational; the request_id is the key
    if body.behavior != "allow" && body.behavior != "deny" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "behavior must be \"allow\" or \"deny\"" })),
        ));
    }
    state
        .perms
        .resolve(
            &req_id,
            Decision {
                behavior: body.behavior,
                message: body.message,
            },
        )
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": e.to_string() })),
            )
        })?;
    Ok(Json(json!({ "ok": true, "request_id": req_id })))
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("{e}") })),
    )
}
