//! Minimal localhost-only HTTP listener — the *only* HTTP server po-k keeps
//! after the Xpo-k cutover (M14 §5.6). It exists solely because CC's hook
//! system and the `po-k mcp` permission subprocess call back over HTTP via
//! `curl`/`reqwest`. Bound to `127.0.0.1`, no auth (local trust boundary).
//!
//! Routes (both reuse the transport-agnostic `core` logic):
//!   - `POST /sessions/{id}/hooks/{event}` — CC lifecycle hook ingestion
//!   - `POST /sessions/{id}/mcp/approve`    — blocking permission decision

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::core::{CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

fn adapt(r: CoreResult<CoreResponse>) -> (StatusCode, Json<Value>) {
    match r {
        Ok(ok) => (
            StatusCode::from_u16(ok.status).unwrap_or(StatusCode::OK),
            Json(ok.body),
        ),
        Err(e) => (
            StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(e.body()),
        ),
    }
}

async fn ingest(
    State(state): State<AppState>,
    Path((sid, event)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    adapt(crate::core::hooks::ingest(&state, &sid, &event, payload).await)
}

#[derive(Debug, Deserialize)]
struct ApproveBody {
    tool_name: String,
    #[serde(default)]
    input: Value,
}

async fn approve(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<ApproveBody>,
) -> (StatusCode, Json<Value>) {
    adapt(crate::core::perms::approve(&state, &sid, &body.tool_name, body.input).await)
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/sessions/{id}/hooks/{event}", post(ingest))
        .route("/sessions/{id}/mcp/approve", post(approve))
        .fallback(|| async {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "po-k hook listener: unknown route" })),
            )
        })
        .with_state(state)
}

/// Bind and serve the listener. Errors only on a bind failure; runs until the
/// process exits.
pub async fn serve(state: AppState, bind: &str) -> Result<()> {
    let addr = SocketAddr::from_str(bind).with_context(|| format!("parsing hook bind {bind:?}"))?;
    if !addr.ip().is_loopback() {
        tracing::warn!(%addr, "hook listener bound to non-loopback — it has NO auth");
    }
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding hook listener {addr}"))?;
    tracing::info!(%addr, "po-k hook listener ready");
    axum::serve(listener, router(state))
        .await
        .context("hook listener serve")?;
    Ok(())
}

// Used as the `_ = serve(...)` ignore guard for CoreError import in fallbacks.
#[allow(dead_code)]
fn _force_use(_: CoreError) {}
