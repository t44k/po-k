//! `POST /sessions/:id/hooks/:event` — thin adapter over [`crate::core::hooks`].

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;

use crate::state::AppState;

pub async fn ingest(
    State(state): State<AppState>,
    Path((sid, event)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::hooks::ingest(&state, &sid, &event, payload).await)
}
