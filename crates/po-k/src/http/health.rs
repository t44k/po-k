//! `GET /health` — unauthenticated liveness.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::state::AppState;

pub async fn handler(State(state): State<AppState>) -> Json<Value> {
    let sessions = state.sessions.list().await.len();
    let hosts = crate::hub::store::list_hosts(&state.db).await.map(|h| h.len()).unwrap_or(0);
    let watches = crate::hub::store::active_watches(&state.db).await.map(|w| w.len()).unwrap_or(0);
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "sessions": sessions,
        "hosts": hosts,
        "watches": watches,
    }))
}
