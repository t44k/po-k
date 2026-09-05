//! `POST /sessions/{id}/hooks/{event}` — CC lifecycle hook ingestion.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;

use super::body::PokJson;
use crate::state::AppState;

pub async fn ingest(
    State(state): State<AppState>,
    Path((sid, event)): Path<(String, String)>,
    PokJson(payload): PokJson<Value>,
) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::hooks::ingest(&state, &sid, &event, payload).await)
}
