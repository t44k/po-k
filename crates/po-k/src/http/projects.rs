//! `GET /projects` — thin adapter over [`crate::core::projects`].

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::Value;

use crate::state::AppState;

pub async fn list(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::projects::list(&state).await)
}
