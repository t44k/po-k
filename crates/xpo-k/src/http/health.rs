//! Health + registry views (Xpo-k's own, not routed to po-k).

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::state::XState;

pub async fn health(State(st): State<XState>) -> Json<Value> {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "connected_pok": st.registry.connected_count(),
    }))
}

pub async fn registry(State(st): State<XState>) -> Json<Value> {
    Json(st.registry.list())
}
