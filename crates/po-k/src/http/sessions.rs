//! Session CRUD + capabilities — thin adapters over `core::sessions`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;

use super::body::PokJson;
use crate::core::sessions::CreateRequest;
use crate::state::AppState;

pub async fn create(State(state): State<AppState>, PokJson(body): PokJson<CreateRequest>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::sessions::create(&state, body).await)
}

pub async fn list(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::sessions::list(&state).await)
}

pub async fn detail(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::sessions::get(&state, &sid).await)
}

pub async fn delete(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::sessions::delete(&state, &sid).await)
}

pub async fn capabilities(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::capabilities::get(&state, &sid).await)
}
