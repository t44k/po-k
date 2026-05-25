//! `GET /projects` — configured project list with per-project running session
//! ids drawn from the in-memory `Registry`.

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct ProjectView {
    pub name: String,
    pub cwd: String,
    pub session_ids: Vec<String>,
}

pub async fn list(State(state): State<AppState>) -> Json<Vec<ProjectView>> {
    let projects = state.projects().await;
    let mut out = Vec::with_capacity(projects.len());
    for p in projects {
        let session_ids = state.sessions.ids_for_project(&p.name).await;
        out.push(ProjectView {
            name: p.name,
            cwd: p.cwd,
            session_ids,
        });
    }
    Json(out)
}
