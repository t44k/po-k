//! Permission round-trip — thin adapters over `core::perms`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use super::body::PokJson;
use crate::state::AppState;

/// `POST /sessions/{id}/mcp/approve` (called by `po-k cc-mcp`).
#[derive(Debug, Deserialize)]
pub struct ApproveBody {
    pub tool_name: String,
    #[serde(default)]
    pub input: Value,
}

pub async fn approve(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    PokJson(body): PokJson<ApproveBody>,
) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::perms::approve(&state, &sid, &body.tool_name, body.input).await)
}

/// `POST /sessions/{id}/permission_requests/{req_id}`
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ResolveBody {
    /// `allow` or `deny`.
    pub behavior: String,
    /// Optional reason shown to CC.
    #[serde(default)]
    pub message: Option<String>,
}

pub async fn resolve(
    State(state): State<AppState>,
    Path((_sid, req_id)): Path<(String, String)>,
    PokJson(body): PokJson<ResolveBody>,
) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::perms::resolve(&state, &req_id, &body.behavior, body.message).await)
}
