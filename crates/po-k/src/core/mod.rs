//! Transport-agnostic business logic (M14).
//!
//! Every operation po-k exposes lives here as a plain async function taking
//! `&AppState` plus typed parameters and returning [`CoreResult`]. The Axum
//! handlers in `http/` and (from Phase 2) the WebSocket dispatcher are both
//! thin adapters over these functions — neither owns any business logic, so
//! the two transports can never drift and the logic is unit-testable without a
//! server stack.

use serde_json::{json, Value};

pub mod capabilities;
pub mod control;
pub mod events;
pub mod hooks;
pub mod messages;
pub mod perms;
pub mod projects;
pub mod sessions;

/// A successful operation result: an HTTP-ish status plus a JSON body. The
/// status preserves the original HTTP semantics (e.g. 201 for session create)
/// so both transports report it identically.
#[derive(Debug, Clone)]
pub struct CoreResponse {
    pub status: u16,
    pub body: Value,
}

impl CoreResponse {
    pub fn ok(body: Value) -> Self {
        Self { status: 200, body }
    }
    pub fn created(body: Value) -> Self {
        Self { status: 201, body }
    }
}

/// Operation failures, mapped to a status + JSON error body by each transport.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("{0}")]
    NotFound(String),
    /// 409. Carries an extra body (e.g. the existing `session_id`) merged into
    /// the error object so the response stays byte-compatible with the old API.
    #[error("{message}")]
    Conflict { message: String, body: Value },
    #[error("{0}")]
    BadRequest(String),
    #[error("operation timed out")]
    Timeout,
    #[error("{0:#}")]
    Internal(#[from] anyhow::Error),
}

pub type CoreResult<T> = Result<T, CoreError>;

impl CoreError {
    pub fn not_found(sid: &str) -> Self {
        CoreError::NotFound(format!("session {sid} not found"))
    }

    /// HTTP status code for this error.
    pub fn status(&self) -> u16 {
        match self {
            CoreError::NotFound(_) => 404,
            CoreError::Conflict { .. } => 409,
            CoreError::BadRequest(_) => 400,
            CoreError::Timeout => 504,
            CoreError::Internal(_) => 500,
        }
    }

    /// The JSON error body. For `Conflict`, merges the extra fields in.
    pub fn body(&self) -> Value {
        match self {
            CoreError::Conflict { message, body } => {
                let mut obj = json!({ "error": message });
                if let (Value::Object(o), Value::Object(extra)) = (&mut obj, body) {
                    for (k, v) in extra {
                        o.insert(k.clone(), v.clone());
                    }
                }
                obj
            }
            other => json!({ "error": other.to_string() }),
        }
    }
}

/// Map an `anyhow::Error` from infrastructure into a CoreError::Internal.
pub fn internal<E: Into<anyhow::Error>>(e: E) -> CoreError {
    CoreError::Internal(e.into())
}
