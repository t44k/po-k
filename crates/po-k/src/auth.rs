//! Bearer-token auth middleware.
//!
//! - The token is loaded from `auth.bearer_token_file` (default `~/.config/po-k/auth.token`).
//! - `/health` is the only unauthenticated route — wire it on a separate router.
//! - Everything else requires `Authorization: Bearer <token>`.

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
    #[cfg(test)]
    pub fn __test_new(value: String) -> Self {
        Self {
            value: Arc::new(value),
        }
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let trimmed = raw.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("{} is empty — run `po-k init` to generate a token", path.display());
        }
        Ok(Self {
            value: Arc::new(trimmed),
        })
    }

    /// Exposed so other crate modules (session spawner, mcp client) can bake
    /// the bearer into per-session hooks.json + mcp.json. Treat the returned
    /// string as sensitive.
    pub fn raw(&self) -> &str {
        &self.value
    }

    /// Constant-time-ish equality. Token comparison doesn't need full timing
    /// resistance for a localhost service, but we still avoid early exits.
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

/// Generate 32 random bytes hex-encoded (64 chars).
pub fn generate_hex_token() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    let mut s = String::with_capacity(64);
    for b in buf {
        s.push_str(&format!("{:02x}", b));
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
    let presented = match presented {
        Some(p) => p,
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "missing or malformed Authorization header — expected `Authorization: Bearer <token>` (token at ~/.config/po-k/auth.token)"
                })),
            ));
        }
    };
    if !token.matches(presented) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid bearer token" })),
        ));
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact() {
        let t = Token { value: Arc::new("abc".into()) };
        assert!(t.matches("abc"));
        assert!(!t.matches("abd"));
        assert!(!t.matches("ab"));
        assert!(!t.matches("abcd"));
    }

    #[test]
    fn generated_token_is_64_hex_chars() {
        let t = generate_hex_token();
        assert_eq!(t.len(), 64);
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
