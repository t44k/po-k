//! Project listing.

use serde_json::json;

use super::{CoreResponse, CoreResult};
use crate::state::AppState;

pub async fn list(state: &AppState) -> CoreResult<CoreResponse> {
    let projects = state.projects().await;
    let mut out = Vec::with_capacity(projects.len());
    for p in projects {
        let session_ids = state.sessions.ids_for_project(&p.name).await;
        out.push(json!({
            "name": p.name,
            "cwd": p.cwd,
            "session_ids": session_ids,
        }));
    }
    Ok(CoreResponse::ok(json!(out)))
}
