//! `GET /help` — the API reference as Markdown (`text/plain` by default, a
//! `{format, version, content}` JSON wrapper with `Accept: application/json`).
//! `GET /docs` is the machine-readable twin; a test checks every route in the
//! table is named here.

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub const HELP_MD: &str = include_str!("help.md");

pub async fn handler(headers: HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/json"))
        .unwrap_or(false);
    if wants_json {
        Json(json!({
            "format": "markdown",
            "version": env!("CARGO_PKG_VERSION"),
            "content": HELP_MD,
        }))
        .into_response()
    } else {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"))],
            HELP_MD,
        )
            .into_response()
    }
}
