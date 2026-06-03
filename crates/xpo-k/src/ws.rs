//! WebSocket server: accepts po-k connections at `/ws`, handles registration,
//! and bridges orchestrator HTTP calls to po-k. Filled in at M2.8.

use axum::Router;

use crate::state::XState;

pub fn router(_state: XState) -> Router {
    Router::new()
}
