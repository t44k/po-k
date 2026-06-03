//! Message input endpoints — thin adapters over [`crate::core::messages`].

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct MessageBody {
    pub text: String,
}

pub async fn message(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<MessageBody>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::messages::send(&state, &sid, &body.text).await)
}

pub async fn interrupt(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::messages::interrupt(&state, &sid).await)
}

pub async fn clear(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::messages::clear(&state, &sid).await)
}

#[derive(Debug, Deserialize)]
pub struct FileBody {
    pub filename: String,
    pub content_base64: String,
}

pub async fn upload_file(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<FileBody>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(
        crate::core::messages::upload_file(&state, &sid, &body.filename, &body.content_base64)
            .await,
    )
}
