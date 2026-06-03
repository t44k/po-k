//! Control endpoints (status / wait / pane) — thin adapters over
//! [`crate::core::control`].

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::state::AppState;

pub async fn status(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::control::status(&state, &sid).await)
}

#[derive(Debug, Deserialize, Default)]
pub struct WaitQuery {
    #[serde(default)]
    pub since: Option<i64>,
    #[serde(default)]
    pub timeout: Option<u64>,
}

pub async fn wait(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<WaitQuery>,
) -> (StatusCode, Json<Value>) {
    let since = q.since.unwrap_or(0);
    let timeout = crate::core::control::wait_defaults(q.timeout);
    crate::http::adapt(crate::core::control::wait(&state, &sid, since, timeout).await)
}

pub async fn pane(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::control::pane(&state, &sid).await)
}
