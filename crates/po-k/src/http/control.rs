//! status / wait / pane — thin adapters over `core::control`.

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;

use super::query::wait_params;
use crate::state::AppState;

pub async fn status(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::control::status(&state, &sid).await)
}

pub async fn wait(State(state): State<AppState>, Path(sid): Path<String>, RawQuery(q): RawQuery) -> (StatusCode, Json<Value>) {
    let (since, timeout) = wait_params(q.as_deref().unwrap_or(""));
    super::adapt(crate::core::control::wait(&state, &sid, since, timeout).await)
}

pub async fn pane(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::control::pane(&state, &sid).await)
}
