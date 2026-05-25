//! `GET /health` — unauthenticated liveness.

use axum::Json;
use serde_json::{json, Value};

pub async fn handler() -> Json<Value> {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}
