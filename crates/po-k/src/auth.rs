//! Bearer-token auth.
//!
//! One token per po-k (in a fleet: one token for every po-k). Loaded from
//! `auth.bearer_token_file`; `POK_TOKEN` can seed that file at startup. Every
//! route except `/health`, `/help` and `/docs` requires
//! `Authorization: Bearer <token>` — including CC's own hook + permission
//! callbacks, which already send it.

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::Json;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
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
            anyhow::bail!(
                "{} is empty — run `po-k init` or set POK_TOKEN to generate one",
                path.display()
            );
        }
        Ok(Self {
            value: Arc::new(trimmed),
        })
    }

    /// The raw secret — baked into per-session hooks.json / mcp.json and used
    /// by the hub to call remote po-ks. Treat as sensitive.
    pub fn raw(&self) -> &str {
        &self.value
    }

    /// Constant-time-ish equality (no early exit on the first mismatch).
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

/// 32 random bytes, hex-encoded (64 chars).
pub fn generate_hex_token() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Write a token file with mode 0600 (creating parent dirs).
pub fn write_token_file(path: &Path, value: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(path, value.trim()).with_context(|| format!("writing {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))?;
    Ok(())
}

/// Extract the bearer from an `Authorization` header value (`Bearer x` or
/// `bearer x`).
pub fn bearer_from_header(value: &str) -> Option<&str> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
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
    let Some(presented) = bearer_from_header(header_value) else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "missing or malformed Authorization header — expected `Authorization: Bearer <token>` (token at ~/.config/po-k/auth.token)"
            })),
        ));
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
        let t = Token::__test_new("abc".into());
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

    #[test]
    fn bearer_prefix_is_case_tolerant() {
        assert_eq!(bearer_from_header("Bearer x"), Some("x"));
        assert_eq!(bearer_from_header("bearer x"), Some("x"));
        assert_eq!(bearer_from_header("Basic x"), None);
    }

    #[test]
    fn token_file_round_trip_is_0600() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sub").join("auth.token");
        write_token_file(&p, "  tok \n").unwrap();
        assert_eq!(Token::from_file(&p).unwrap().raw(), "tok");
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
