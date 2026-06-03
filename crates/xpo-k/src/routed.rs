//! Orchestrator-facing endpoints that mirror po-k's HTTP API but are fulfilled
//! by routing each call to the owning po-k over WebSocket (filled in at M2.8).

use axum::Router;

use crate::state::XState;

pub fn router() -> Router<XState> {
    Router::new()
}
