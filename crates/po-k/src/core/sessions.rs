//! Session lifecycle: create, list, get, delete.

use serde_json::{json, Value};

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::session::{self, RunningSession, SpawnError, SpawnOptions};
use crate::state::AppState;

/// Inputs for creating a session. Mirrors the extended `POST /sessions` body
/// (spec §4.3) — `project` plus optional merged profile and flags.
///
/// **Ad-hoc mode** (when `cc.ad_hoc: true` in config): if `cwd` is set and
/// `project` is empty, a synthetic project is created from the directory name.
/// If `project` is also provided it is used as the session's project name.
#[derive(Debug, Default, serde::Deserialize)]
pub struct CreateRequest {
    #[serde(default)]
    pub project: String,
    /// Explicit working directory. When set, overrides the project's configured
    /// cwd (or creates an ad-hoc project if the name is unknown/empty).
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub profile: Option<crate::profile::Profile>,
    #[serde(default)]
    pub profiles: Vec<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub bare: bool,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
}

pub async fn create(state: &AppState, req: CreateRequest) -> CoreResult<CoreResponse> {
    let opts = SpawnOptions {
        profile: req.profile,
        profile_names: req.profiles,
        agent: req.agent,
        bare: req.bare,
        model: req.model,
        effort: req.effort,
        cwd_override: req.cwd,
    };
    match session::spawn(state, &req.project, opts).await {
        Ok(s) => Ok(CoreResponse::created(view_full(&s))),
        Err(SpawnError::UnknownProject(name)) => {
            Err(CoreError::NotFound(format!("unknown project {name:?}")))
        }
        Err(SpawnError::AdHocDisabled) => Err(CoreError::BadRequest(
            "ad-hoc sessions disabled; set cc.ad_hoc: true in po-k.yaml".into(),
        )),
        Err(SpawnError::AlreadyRunning { project, sid }) => Err(CoreError::Conflict {
            message: format!("a session for project {project:?} is already running"),
            body: json!({ "session_id": sid }),
        }),
        Err(SpawnError::Other(e)) => Err(internal(e)),
    }
}

pub async fn list(state: &AppState) -> CoreResult<CoreResponse> {
    let sessions = state.sessions.list().await;
    Ok(CoreResponse::ok(json!(sessions
        .iter()
        .map(view_full)
        .collect::<Vec<_>>())))
}

pub async fn get(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    state
        .sessions
        .get(sid)
        .await
        .map(|s| CoreResponse::ok(view_full(&s)))
        .ok_or_else(|| CoreError::not_found(sid))
}

pub async fn delete(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    if state.sessions.get(sid).await.is_none() {
        return Err(CoreError::not_found(sid));
    }
    session::kill(state, sid).await.map_err(internal)?;
    Ok(CoreResponse::ok(json!({ "ok": true, "session_id": sid })))
}

pub fn view_full(s: &RunningSession) -> Value {
    json!({
        "session_id": s.sid,
        "project": s.project,
        "cwd": s.cwd,
        "zellij_session": s.zellij_session,
        "model": s.model,
        "effort": s.effort,
        "started_at": s.started_at,
        "pid": s.pid,
        "hooks_path": s.hooks_path,
        "mcp_path": s.mcp_path,
        "profiles": s.profiles,
        "plugin_dir": s.plugin_dir,
    })
}
