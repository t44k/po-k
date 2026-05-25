//! Session lifecycle: spawn CC inside a per-project zellij session, track its
//! state in memory + in the events.db `sessions` table, tear it all down on
//! delete.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{CcDefaults, Config, Project, Zellij};
use crate::events_store::{self, Db, SessionRow};
use crate::state::AppState;
use crate::zellij;

#[derive(Debug, Clone, Serialize)]
pub struct RunningSession {
    pub sid: String,
    pub project: String,
    pub cwd: String,
    pub zellij_session: String,
    pub model: String,
    pub effort: String,
    pub started_at: String,
    pub hooks_path: String,
    pub mcp_path: String,
    /// Resolved asynchronously after CC starts. v1 is None.
    pub pid: Option<i64>,
}

#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, RunningSession>>>,
}

impl Registry {
    pub async fn list(&self) -> Vec<RunningSession> {
        self.inner.lock().await.values().cloned().collect()
    }

    pub async fn get(&self, sid: &str) -> Option<RunningSession> {
        self.inner.lock().await.get(sid).cloned()
    }

    pub async fn ids_for_project(&self, project: &str) -> Vec<String> {
        self.inner
            .lock()
            .await
            .values()
            .filter(|s| s.project == project)
            .map(|s| s.sid.clone())
            .collect()
    }

    async fn insert(&self, s: RunningSession) {
        self.inner.lock().await.insert(s.sid.clone(), s);
    }

    async fn remove(&self, sid: &str) -> Option<RunningSession> {
        self.inner.lock().await.remove(sid)
    }
}

/// Errors that can surface to the HTTP layer; the `serve` layer maps these to
/// HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("project {0:?} not found in config")]
    UnknownProject(String),
    #[error("spawning session: {0}")]
    Other(#[from] anyhow::Error),
}

pub async fn spawn(state: &AppState, project_name: &str) -> Result<RunningSession, SpawnError> {
    let snapshot = state.config.read().await.clone();
    let project = snapshot
        .projects
        .iter()
        .find(|p| p.name == project_name)
        .ok_or_else(|| SpawnError::UnknownProject(project_name.to_string()))?
        .clone();
    spawn_for_project(state, &snapshot, &project)
        .await
        .map_err(SpawnError::Other)
}

async fn spawn_for_project(
    state: &AppState,
    cfg: &Config,
    project: &Project,
) -> Result<RunningSession> {
    let sid = Uuid::new_v4().to_string();
    let zellij_session_name = project.zellij_session_name(&cfg.zellij);
    let session_dir = crate::config::expand_path(&format!("~/.cache/po-k/sessions/{sid}"));
    std::fs::create_dir_all(&session_dir)
        .with_context(|| format!("creating {}", session_dir.display()))?;

    let hooks_path = session_dir.join("hooks.json");
    let mcp_path = session_dir.join("mcp.json");
    let token_file = crate::config::expand_path(&cfg.auth.bearer_token_file);

    std::fs::write(
        &hooks_path,
        render_hooks_json(&cfg.server.base_url, &sid, state.token.raw()),
    )
    .with_context(|| format!("writing {}", hooks_path.display()))?;
    std::fs::write(
        &mcp_path,
        render_mcp_json(&sid, &cfg.server.base_url, &token_file),
    )
    .with_context(|| format!("writing {}", mcp_path.display()))?;

    zellij::ensure_session(&zellij_session_name)
        .await
        .with_context(|| format!("ensuring zellij session {zellij_session_name:?}"))?;

    let model = project.model(&cfg.cc).to_string();
    let effort = project.effort(&cfg.cc).to_string();
    let cmd = render_bootstrap(
        &project.cwd,
        &sid,
        &model,
        &effort,
        &cfg.cc,
        project,
        &hooks_path,
        &mcp_path,
    );
    // Submit the bootstrap line into the pane. Single write_chars with a
    // trailing newline so the shell submits immediately.
    let payload = format!("{cmd}\n");
    zellij::write_chars(&zellij_session_name, &payload).await?;

    let started_at = events_store::now_iso();
    let row = SessionRow {
        sid: sid.clone(),
        project: project.name.clone(),
        cwd: project.cwd.clone(),
        zellij_session: zellij_session_name.clone(),
        model: Some(model.clone()),
        effort: Some(effort.clone()),
        started_at: started_at.clone(),
        ended_at: None,
        pid: None,
        last_event_seq: 0,
    };
    events_store::insert_session(&state.db, &row).await?;

    append_lifecycle_event(
        &state.db,
        &sid,
        "cc_started",
        &json!({
            "model": model,
            "effort": effort,
            "cwd": project.cwd,
            "zellij_session": zellij_session_name,
        }),
    )
    .await?;

    let running = RunningSession {
        sid: sid.clone(),
        project: project.name.clone(),
        cwd: project.cwd.clone(),
        zellij_session: zellij_session_name,
        model,
        effort,
        started_at,
        hooks_path: hooks_path.to_string_lossy().into_owned(),
        mcp_path: mcp_path.to_string_lossy().into_owned(),
        pid: None,
    };
    state.sessions.insert(running.clone()).await;
    state.bus.notify(&sid).await;

    // Per-session JSONL tailer projects CC's transcript lines into events rows.
    crate::jsonl_tail::spawn(
        state.db.clone(),
        state.bus.clone(),
        sid.clone(),
        project.cwd.clone(),
    );

    Ok(running)
}

pub async fn kill(state: &AppState, sid: &str) -> Result<()> {
    let running = state
        .sessions
        .get(sid)
        .await
        .ok_or_else(|| anyhow::anyhow!("session {sid} not found"))?;

    // Try graceful exit first: /exit\n into the pane.
    let _ = zellij::write_chars(&running.zellij_session, "/exit\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Always reap the zellij session so a future start re-creates it cleanly.
    let _ = zellij::kill_session(&running.zellij_session).await;

    let _ = std::fs::remove_dir_all(crate::config::expand_path(&format!(
        "~/.cache/po-k/sessions/{sid}"
    )));

    state.sessions.remove(sid).await;
    let ts = events_store::now_iso();
    let _ = append_lifecycle_event(&state.db, sid, "cc_exited", &json!({})).await;
    state.bus.notify(sid).await;
    events_store::mark_session_ended(&state.db, sid, &ts).await?;
    state.bus.drop_session(sid).await;
    Ok(())
}

async fn append_lifecycle_event(db: &Db, sid: &str, kind: &str, payload: &Value) -> Result<()> {
    let ts = events_store::now_iso();
    events_store::append_event(db, sid, &ts, kind, payload).await?;
    Ok(())
}

pub fn render_hooks_json(base_url: &str, sid: &str, token: &str) -> String {
    let mk = |event: &str| -> Value {
        let url = format!("{base_url}/sessions/{sid}/hooks/{event}");
        let command = format!(
            "curl -sX POST '{url}' -H 'authorization: bearer {token}' --data-binary @-",
        );
        json!({
            "matcher": "",
            "hooks": [{ "type": "command", "command": command }]
        })
    };
    let body = json!({
        "hooks": {
            "UserPromptSubmit": [mk("UserPromptSubmit")],
            "Stop":             [mk("Stop")],
            "SubagentStop":     [mk("SubagentStop")],
            "PostToolUse":      [mk("PostToolUse")],
            "Notification":     [mk("Notification")],
            "SessionEnd":       [mk("SessionEnd")],
        }
    });
    serde_json::to_string_pretty(&body).expect("hooks.json serialize")
}

pub fn render_mcp_json(sid: &str, base_url: &str, token_file: &std::path::Path) -> String {
    let body = json!({
        "mcpServers": {
            "po-k": {
                "command": "po-k",
                "args": [
                    "mcp",
                    "--session-id", sid,
                    "--base-url", base_url,
                    "--token-file", token_file.to_string_lossy().into_owned(),
                ]
            }
        }
    });
    serde_json::to_string_pretty(&body).expect("mcp.json serialize")
}

#[allow(clippy::too_many_arguments)]
pub fn render_bootstrap(
    cwd: &str,
    sid: &str,
    model: &str,
    effort: &str,
    cc: &CcDefaults,
    project: &Project,
    hooks_path: &std::path::Path,
    mcp_path: &std::path::Path,
) -> String {
    let mut add_dirs: Vec<String> = if project.add_dirs.is_empty() {
        vec![cwd.to_string()]
    } else {
        project.add_dirs.clone()
    };
    add_dirs.dedup();
    let add_args = add_dirs
        .iter()
        .map(|d| format!("--add-dir {}", shell_quote(d)))
        .collect::<Vec<_>>()
        .join(" ");
    let mut parts = vec![
        format!("cd {} &&", shell_quote(cwd)),
        "exec claude".to_string(),
        format!("--session-id {sid}"),
        format!("--model {}", shell_quote(model)),
        format!("--effort {}", shell_quote(effort)),
        format!("--permission-mode {}", shell_quote(&cc.permission_mode)),
        "--permission-prompt-tool mcp__po-k__approve".to_string(),
        format!("--mcp-config {}", shell_quote(&mcp_path.to_string_lossy())),
        format!("--settings {}", shell_quote(&hooks_path.to_string_lossy())),
    ];
    if cc.disable_slash_commands {
        parts.push("--disable-slash-commands".to_string());
    }
    if !add_args.is_empty() {
        parts.push(add_args);
    }
    parts.join(" ")
}

/// Minimal POSIX single-quote shell escaper.
fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | ','))
    {
        return s.to_string();
    }
    let escaped = s.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

// `Project` already has `model()` / `effort()` helpers — re-export under the
// shape spawn_for_project expects.
trait _ProjectExt {}
impl _ProjectExt for Project {}

// Touch unused-but-imported symbols in test mode only.
#[cfg(test)]
fn _unused(_: Zellij) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn renders_hooks_json_with_token_inline() {
        let s = render_hooks_json("http://127.0.0.1:7070", "abc-123", "TOK");
        assert!(s.contains("http://127.0.0.1:7070/sessions/abc-123/hooks/Stop"));
        assert!(s.contains("authorization: bearer TOK"));
        // Every hook event we promise to install must be present.
        for event in [
            "UserPromptSubmit",
            "Stop",
            "SubagentStop",
            "PostToolUse",
            "Notification",
            "SessionEnd",
        ] {
            assert!(s.contains(event), "missing event {event}");
        }
    }

    #[test]
    fn renders_mcp_json_with_session_args() {
        let s = render_mcp_json(
            "abc-123",
            "http://127.0.0.1:7070",
            &PathBuf::from("/home/me/.config/po-k/auth.token"),
        );
        assert!(s.contains("abc-123"));
        assert!(s.contains("/home/me/.config/po-k/auth.token"));
        assert!(s.contains("\"command\": \"po-k\""));
        assert!(s.contains("\"mcp\""));
    }

    #[test]
    fn bootstrap_contains_all_required_flags() {
        let cc = CcDefaults::default();
        let project = Project {
            name: "po-k".into(),
            cwd: "/workspace".into(),
            model: None,
            effort: None,
            add_dirs: vec![],
            zellij_session: None,
        };
        let bash = render_bootstrap(
            "/workspace",
            "abc-123",
            "sonnet",
            "medium",
            &cc,
            &project,
            std::path::Path::new("/tmp/h.json"),
            std::path::Path::new("/tmp/m.json"),
        );
        assert!(bash.starts_with("cd /workspace && exec claude"));
        assert!(bash.contains("--session-id abc-123"));
        assert!(bash.contains("--model sonnet"));
        assert!(bash.contains("--effort medium"));
        assert!(bash.contains("--permission-mode acceptEdits"));
        assert!(bash.contains("--permission-prompt-tool mcp__po-k__approve"));
        assert!(bash.contains("--mcp-config /tmp/m.json"));
        assert!(bash.contains("--settings /tmp/h.json"));
        assert!(bash.contains("--disable-slash-commands"));
        assert!(bash.contains("--add-dir /workspace"));
    }

    #[test]
    fn bootstrap_quotes_paths_with_spaces() {
        let cc = CcDefaults::default();
        let project = Project {
            name: "p".into(),
            cwd: "/home/me/with space".into(),
            model: None,
            effort: None,
            add_dirs: vec!["/home/me/with space".into()],
            zellij_session: None,
        };
        let bash = render_bootstrap(
            "/home/me/with space",
            "sid",
            "sonnet",
            "medium",
            &cc,
            &project,
            std::path::Path::new("/tmp/h.json"),
            std::path::Path::new("/tmp/m.json"),
        );
        assert!(bash.contains("'/home/me/with space'"));
    }

    #[test]
    fn shell_quote_leaves_safe_chars_alone() {
        assert_eq!(shell_quote("/abc/def"), "/abc/def");
        assert_eq!(shell_quote("safe-name.1_2"), "safe-name.1_2");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("with space"), "'with space'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }
}
