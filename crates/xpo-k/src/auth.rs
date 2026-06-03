//! Bearer-token auth for Xpo-k's HTTP API + WebSocket upgrade. Xpo-k is the
//! only authenticated entry point (po-k has no HTTP auth in the new design).

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Token {
    value: Arc<String>,
}

impl Token {
    pub fn new(value: String) -> Self {
        Self {
            value: Arc::new(value),
        }
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let trimmed = raw.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("{} is empty — run `xpo-k init`", path.display());
        }
        Ok(Self::new(trimmed))
    }

    pub fn matches(&self, candidate: &str) -> bool {
        let a = self.value.as_bytes();
        let b = candidate.as_bytes();
        if a.len() != b.len() {
            return false;
        }
        let mut diff: u8 = 0;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }
}

pub fn generate_hex_token() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    let mut s = String::with_capacity(64);
    for b in buf {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub async fn require_bearer(
    State(token): State<Token>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let header_value = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let presented = header_value
        .strip_prefix("Bearer ")
        .or_else(|| header_value.strip_prefix("bearer "));
    match presented {
        Some(p) if token.matches(p) => Ok(next.run(req).await),
        Some(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid bearer token" })),
        )),
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or malformed Authorization header" })),
        )),
    }
}
