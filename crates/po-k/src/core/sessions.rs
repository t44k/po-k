//! Session lifecycle: create, list, get, delete.

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::defaults;
use crate::profile::McpServer;
use crate::session::{self, RunningSession, SpawnError, SpawnRequest};
use crate::state::AppState;

/// `POST /sessions` body. Everything a session needs, nothing configured on
/// the box. Unknown fields are rejected (400) so a typo cannot silently fall
/// back to a default.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    /// Absolute working directory for Claude Code. Created if missing.
    pub cwd: String,
    /// Session name. Defaults to a slug of the directory's basename. The
    /// zellij session is `po-k-<name>`; only one live session per name (409).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `--model` (alias or full id). Default: `fable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// `--effort`. Default: `xhigh`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// `--permission-mode`. Default: `bypassPermissions`. Permission prompts
    /// that still occur are routed to the orchestrator as
    /// `permission_request` events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,
    /// CC plugins to load: absolute directory or `.zip` paths on this box
    /// (`--plugin-dir`) or `http(s)://` URLs (`--plugin-url`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    /// Extra MCP servers for CC, keyed by name, in `.mcp.json` entry shape.
    /// The name `po-k` is reserved.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub mcp_servers: IndexMap<String, McpServer>,
    /// `--agent`: main agent to run (must exist in a loaded plugin).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Extra `--add-dir` entries (absolute). `cwd` is always included.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add_dirs: Vec<String>,
    /// Appended to CC's system prompt (`--append-system-prompt-file`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Reserved. `true` is rejected: `--bare` disables the hooks po-k's status
    /// and wait depend on.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub bare: bool,
}

impl CreateRequest {
    pub fn json_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(CreateRequest)).unwrap_or(json!({}))
    }
}

fn bad(msg: impl Into<String>) -> CoreError {
    CoreError::BadRequest(msg.into())
}

/// Field-level validation. Returns the first problem as a 400 naming the field.
pub fn validate(req: &CreateRequest) -> Result<(), CoreError> {
    if req.cwd.is_empty() {
        return Err(bad("cwd is required"));
    }
    if !req.cwd.starts_with('/') {
        return Err(bad(format!("cwd must be an absolute path, got {:?}", req.cwd)));
    }
    if let Some(name) = &req.name {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')) {
            return Err(bad(format!("name must match [A-Za-z0-9._-]+, got {name:?}")));
        }
    }
    if let Some(pm) = &req.permission_mode {
        if !defaults::PERMISSION_MODES.contains(&pm.as_str()) {
            return Err(bad(format!(
                "permission_mode {pm:?} is not one of {:?}",
                defaults::PERMISSION_MODES
            )));
        }
    }
    for d in &req.add_dirs {
        if !d.starts_with('/') {
            return Err(bad(format!("add_dirs entries must be absolute paths, got {d:?}")));
        }
    }
    for p in &req.plugins {
        if p.starts_with("http://") || p.starts_with("https://") {
            continue;
        }
        if !p.starts_with('/') {
            return Err(bad(format!(
                "plugins entries must be absolute paths or http(s) URLs, got {p:?}"
            )));
        }
        if !std::path::Path::new(p).exists() {
            return Err(bad(format!("plugin path {p:?} does not exist on this box")));
        }
    }
    for (name, s) in &req.mcp_servers {
        if name == "po-k" {
            return Err(bad("mcp_servers name \"po-k\" is reserved"));
        }
        if s.command.is_none() && s.url.is_none() {
            return Err(bad(format!("mcp_servers.{name} needs a command or a url")));
        }
        if !matches!(s.kind.as_str(), "stdio" | "http" | "sse") {
            return Err(bad(format!("mcp_servers.{name}.type must be stdio, http or sse")));
        }
    }
    if req.bare {
        return Err(bad("bare is not supported: --bare disables the CC hooks po-k's status/wait rely on"));
    }
    Ok(())
}

pub async fn create(state: &AppState, req: CreateRequest) -> CoreResult<CoreResponse> {
    validate(&req)?;
    let spawn_req = SpawnRequest {
        cwd: req.cwd,
        name: req.name,
        model: req.model,
        effort: req.effort,
        permission_mode: req.permission_mode,
        plugins: req.plugins,
        mcp_servers: req.mcp_servers,
        agent: req.agent,
        add_dirs: req.add_dirs,
        system_prompt: req.system_prompt,
    };
    match session::spawn(state, spawn_req).await {
        Ok(s) => Ok(CoreResponse::created(view_full(&s))),
        Err(SpawnError::AlreadyRunning { name, sid }) => Err(CoreError::Conflict {
            message: format!("a session named {name:?} is already running; delete it or pass a different name"),
            body: json!({ "session_id": sid }),
        }),
        Err(SpawnError::Other(e)) => Err(internal(e)),
    }
}

pub async fn list(state: &AppState) -> CoreResult<CoreResponse> {
    let sessions = state.sessions.list().await;
    Ok(CoreResponse::ok(json!(sessions.iter().map(view_full).collect::<Vec<_>>())))
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
        "name": s.name,
        "cwd": s.cwd,
        "zellij_session": s.zellij_session,
        "model": s.model,
        "effort": s.effort,
        "permission_mode": s.permission_mode,
        "agent": s.agent,
        "plugins": s.plugins,
        "mcp_servers": s.mcp_servers,
        "started_at": s.started_at,
        "pid": s.pid,
        "hooks_path": s.hooks_path,
        "mcp_path": s.mcp_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(cwd: &str) -> CreateRequest {
        CreateRequest {
            cwd: cwd.into(),
            ..Default::default()
        }
    }

    #[test]
    fn schema_requires_cwd_and_forbids_unknown_fields() {
        let s = CreateRequest::json_schema();
        assert_eq!(s["required"], json!(["cwd"]));
        assert_eq!(s["additionalProperties"], false);
        let props = s["properties"].as_object().unwrap();
        for key in ["cwd", "name", "model", "effort", "permission_mode", "plugins", "mcp_servers", "agent", "add_dirs", "system_prompt"] {
            assert!(props.contains_key(key), "schema lacks {key}");
        }
    }

    #[test]
    fn unknown_field_is_a_deserialize_error() {
        let e = serde_json::from_str::<CreateRequest>(r#"{"cwd":"/x","project":"p"}"#).unwrap_err();
        assert!(e.to_string().contains("project"), "{e}");
    }

    #[test]
    fn validation_names_the_offending_field() {
        assert!(validate(&req("/tmp")).is_ok());
        let e = validate(&req("relative")).unwrap_err();
        assert!(e.to_string().contains("cwd"));
        let mut r = req("/tmp");
        r.permission_mode = Some("yolo".into());
        assert!(validate(&r).unwrap_err().to_string().contains("permission_mode"));
        let mut r = req("/tmp");
        r.plugins = vec!["relative/plugin".into()];
        assert!(validate(&r).unwrap_err().to_string().contains("plugins"));
        let mut r = req("/tmp");
        r.plugins = vec!["/definitely/not/here".into()];
        assert!(validate(&r).unwrap_err().to_string().contains("does not exist"));
        let mut r = req("/tmp");
        r.plugins = vec!["https://example.com/p.zip".into()];
        assert!(validate(&r).is_ok());
        let mut r = req("/tmp");
        r.mcp_servers.insert("po-k".into(), McpServer { command: Some("x".into()), ..Default::default() });
        assert!(validate(&r).unwrap_err().to_string().contains("reserved"));
        let mut r = req("/tmp");
        r.mcp_servers.insert("db".into(), McpServer::default());
        assert!(validate(&r).unwrap_err().to_string().contains("command or a url"));
        let mut r = req("/tmp");
        r.bare = true;
        assert!(validate(&r).unwrap_err().to_string().contains("bare"));
        let mut r = req("/tmp");
        r.name = Some("has space".into());
        assert!(validate(&r).unwrap_err().to_string().contains("name"));
    }
}
