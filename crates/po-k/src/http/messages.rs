//! Input endpoints — thin adapters over `core::messages`.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use super::body::PokJson;
use crate::state::AppState;

/// `POST /sessions/{id}/messages`
#[derive(Debug, Deserialize, JsonSchema)]
pub struct MessageBody {
    /// The prompt to type into CC. Submitted with Enter.
    pub text: String,
}

pub async fn message(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    PokJson(body): PokJson<MessageBody>,
) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::messages::send(&state, &sid, &body.text).await)
}

pub async fn interrupt(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::messages::interrupt(&state, &sid).await)
}

pub async fn clear(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::messages::clear(&state, &sid).await)
}

/// `POST /sessions/{id}/files`
#[derive(Debug, Deserialize, JsonSchema)]
pub struct FileBody {
    /// Bare file name (no `/`, `\` or `..`). Written to `<cwd>/.po-k-inbox/`.
    pub filename: String,
    /// File content, base64.
    pub content_base64: String,
}

pub async fn upload_file(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    PokJson(body): PokJson<FileBody>,
) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::messages::upload_file(&state, &sid, &body.filename, &body.content_base64).await)
}
