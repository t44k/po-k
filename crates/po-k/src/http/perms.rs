//! Permission round-trip endpoints — thin adapters over [`crate::core::perms`].

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

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
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::perms::approve(&state, &sid, &body.tool_name, body.input).await)
}

#[derive(Debug, Deserialize)]
pub struct ResolveBody {
    pub behavior: String,
    #[serde(default)]
    pub message: Option<String>,
}

pub async fn resolve(
    State(state): State<AppState>,
    Path((_sid, req_id)): Path<(String, String)>,
    Json(body): Json<ResolveBody>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::perms::resolve(&state, &req_id, &body.behavior, body.message).await)
}
