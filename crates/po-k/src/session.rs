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

use crate::config::{Config, Project};
use crate::events_store::{self, SessionRow};
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
    /// Profile names merged into this session (M14). Empty for legacy sessions.
    #[serde(default)]
    pub profiles: Vec<String>,
    /// Generated CC plugin directory (M14). None for legacy sessions.
    #[serde(default)]
    pub plugin_dir: Option<String>,
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

    pub async fn insert(&self, s: RunningSession) {
        self.inner.lock().await.insert(s.sid.clone(), s);
    }

    async fn remove(&self, sid: &str) -> Option<RunningSession> {
        self.inner.lock().await.remove(sid)
    }

    #[cfg(test)]
    pub async fn remove_for_test(&self, sid: &str) {
        self.remove(sid).await;
    }
}

/// Errors that can surface to the HTTP layer; the `serve` layer maps these to
/// HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("project {0:?} not found in config")]
    UnknownProject(String),
    #[error("a session for project {project:?} is already running (sid {sid})")]
    AlreadyRunning { project: String, sid: String },
    #[error("spawning session: {0}")]
    Other(#[from] anyhow::Error),
}

/// Optional profile/flag inputs for a session spawn. The legacy
/// `POST /sessions {"project"}` path uses `SpawnOptions::default()` (no
/// profile, no overrides), preserving the pre-M14 behaviour exactly.
#[derive(Default)]
pub struct SpawnOptions {
    /// Already-merged profile (merging happens on Xpo-k). None = legacy mode.
    pub profile: Option<crate::profile::Profile>,
    /// Profile names recorded against the session (for capabilities + DB).
    pub profile_names: Vec<String>,
    /// `--agent <name>` main agent.
    pub agent: Option<String>,
    /// `--bare` clean-room mode.
    pub bare: bool,
    /// Explicit model override (highest precedence).
    pub model: Option<String>,
    /// Explicit effort override (highest precedence).
    pub effort: Option<String>,
}

pub async fn spawn(
    state: &AppState,
    project_name: &str,
    opts: SpawnOptions,
) -> Result<RunningSession, SpawnError> {
    let snapshot = state.config.read().await.clone();
    let project = snapshot
        .projects
        .iter()
        .find(|p| p.name == project_name)
        .ok_or_else(|| SpawnError::UnknownProject(project_name.to_string()))?
        .clone();
    // One CC per project: every session for a project shares one zellij session
    // (and pane), so a second spawn would type its bootstrap into the pane
    // already running the first session's CC. Refuse instead of clobbering —
    // callers must DELETE the existing session first.
    if let Some(sid) = state
        .sessions
        .ids_for_project(&project.name)
        .await
        .into_iter()
        .next()
    {
        return Err(SpawnError::AlreadyRunning {
            project: project.name.clone(),
            sid,
        });
    }
    spawn_for_project(state, &snapshot, &project, opts)
        .await
        .map_err(SpawnError::Other)
}

async fn spawn_for_project(
    state: &AppState,
    cfg: &Config,
    project: &Project,
    opts: SpawnOptions,
) -> Result<RunningSession> {
    let sid = Uuid::new_v4().to_string();
    let zellij_session_name = project.zellij_session_name(&cfg.zellij);
    let session_dir = crate::config::expand_path(format!("~/.cache/po-k/sessions/{sid}"));
    std::fs::create_dir_all(&session_dir)
        .with_context(|| format!("creating {}", session_dir.display()))?;

    let token_file = crate::config::expand_path(&cfg.auth.bearer_token_file);
    let base_url = cfg.hooks.base_url();

    // model / effort / permission_mode precedence:
    //   explicit override (opts) > profile.settings > project/cc default.
    let profile_settings = opts.profile.as_ref().map(|p| &p.settings);
    let model = opts
        .model
        .clone()
        .or_else(|| profile_settings.and_then(|s| s.model.clone()))
        .unwrap_or_else(|| project.model(&cfg.cc).to_string());
    let effort = opts
        .effort
        .clone()
        .or_else(|| profile_settings.and_then(|s| s.effort.clone()))
        .unwrap_or_else(|| project.effort(&cfg.cc).to_string());
    let permission_mode = profile_settings
        .and_then(|s| s.permission_mode.clone())
        .unwrap_or_else(|| cfg.cc.permission_mode.clone());

    // Lay out the config CC reads. Profile mode → a full plugin directory;
    // legacy mode → the flat hooks.json / mcp.json pair (unchanged behaviour).
    let (hooks_path, mcp_path, plugin_dir): (PathBuf, PathBuf, Option<PathBuf>) =
        if let Some(profile) = &opts.profile {
            let pok = crate::profile::PokHookContext {
                base_url: &base_url,
                token: state.token.raw(),
                token_file: &token_file,
                sid: &sid,
            };
            let paths = crate::profile::generate_plugin_dir(&session_dir, profile, &pok)?;
            let settings_path = session_dir.join("settings.json");
            std::fs::write(
                &settings_path,
                crate::profile::render_settings_json(profile, &pok, opts.agent.as_deref())?,
            )
            .with_context(|| format!("writing {}", settings_path.display()))?;
            (paths.hooks_json, paths.mcp_json, Some(paths.dir))
        } else {
            let hooks_path = session_dir.join("hooks.json");
            let mcp_path = session_dir.join("mcp.json");
            std::fs::write(
                &hooks_path,
                render_hooks_json(&base_url, &sid, state.token.raw()),
            )
            .with_context(|| format!("writing {}", hooks_path.display()))?;
            std::fs::write(&mcp_path, render_mcp_json(&sid, &base_url, &token_file))
                .with_context(|| format!("writing {}", mcp_path.display()))?;
            (hooks_path, mcp_path, None)
        };
    // In profile mode --settings points at the merged settings.json; legacy
    // mode keeps passing hooks.json (which carries only the hooks block).
    let settings_path = if plugin_dir.is_some() {
        session_dir.join("settings.json")
    } else {
        hooks_path.clone()
    };

    zellij::ensure_session(&zellij_session_name)
        .await
        .with_context(|| format!("ensuring zellij session {zellij_session_name:?}"))?;

    let spec = BootstrapSpec {
        cwd: &project.cwd,
        sid: &sid,
        model: &model,
        effort: &effort,
        permission_mode: &permission_mode,
        disable_slash_commands: cfg.cc.disable_slash_commands,
        add_dirs: &project.add_dirs,
        mcp_config: &mcp_path,
        settings: &settings_path,
        plugin_dir: plugin_dir.as_deref(),
        agent: opts.agent.as_deref(),
        bare: opts.bare,
    };
    let cmd = render_bootstrap(&spec);
    // Submit the bootstrap line into the pane via the per-session MCP
    // socket. write_to_focused_pane resolves the right terminal pane on the
    // first tab and forwards the bytes verbatim.
    let payload = format!("{cmd}\n");
    zellij::write_to_focused_pane(&zellij_session_name, &payload).await?;

    let started_at = events_store::now_iso();
    let profiles_json = if opts.profile_names.is_empty() {
        None
    } else {
        serde_json::to_string(&opts.profile_names).ok()
    };
    let plugin_dir_str = plugin_dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());
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
        profiles: profiles_json,
        plugin_dir: plugin_dir_str.clone(),
    };
    events_store::insert_session(&state.db, &row).await?;

    append_lifecycle_event(
        state,
        &sid,
        "cc_started",
        &json!({
            "model": model,
            "effort": effort,
            "cwd": project.cwd,
            "zellij_session": zellij_session_name,
            "profiles": opts.profile_names,
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
        profiles: opts.profile_names.clone(),
        plugin_dir: plugin_dir_str,
    };
    state.sessions.insert(running.clone()).await;

    // Per-session JSONL tailer projects CC's transcript lines into events rows.
    // It waits for the transcript as long as the session is alive (CC only
    // writes it after the first submitted prompt), so it gets the Registry.
    crate::jsonl_tail::spawn(state.clone(), sid.clone(), project.cwd.clone());

    Ok(running)
}

pub async fn kill(state: &AppState, sid: &str) -> Result<()> {
    let running = state
        .sessions
        .get(sid)
        .await
        .ok_or_else(|| anyhow::anyhow!("session {sid} not found"))?;

    // Try graceful exit first: /exit\n into the pane.
    let _ = zellij::write_to_focused_pane(&running.zellij_session, "/exit\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Always reap the zellij session so a future start re-creates it cleanly.
    let _ = zellij::kill_session(&running.zellij_session).await;

    let _ = std::fs::remove_dir_all(crate::config::expand_path(format!(
        "~/.cache/po-k/sessions/{sid}"
    )));

    state.sessions.remove(sid).await;
    let ts = events_store::now_iso();
    let _ = append_lifecycle_event(state, sid, "cc_exited", &json!({})).await;
    events_store::mark_session_ended(&state.db, sid, &ts).await?;
    state.bus.drop_session(sid).await;
    Ok(())
}

/// Append a lifecycle event through the central [`crate::core::events::record`]
/// choke point (DB + bus wake + Xpo-k forward).
pub(crate) async fn append_lifecycle_event(
    state: &AppState,
    sid: &str,
    kind: &str,
    payload: &Value,
) -> Result<()> {
    crate::core::events::record(state, sid, kind, payload).await?;
    Ok(())
}

pub fn render_hooks_json(base_url: &str, sid: &str, token: &str) -> String {
    let mk = |event: &str| -> Value {
        let url = format!("{base_url}/sessions/{sid}/hooks/{event}");
        // `content-type: application/json` is REQUIRED: the ingest handler uses
        // axum's `Json` extractor, which 415s any other content type. Without
        // it the hook silently no-ops (curl still exits 0, so CC logs the hook
        // as a success) and lifecycle events like `stop`/`notification` never
        // reach the server.
        let command = format!(
            "curl -sX POST '{url}' -H 'authorization: bearer {token}' -H 'content-type: application/json' --data-binary @-",
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

/// Everything needed to render the `claude` bootstrap command line. Replaces
/// the old positional-arg form so profile-mode flags (`--plugin-dir`,
/// `--agent`, `--bare`) can be threaded through cleanly.
pub struct BootstrapSpec<'a> {
    pub cwd: &'a str,
    pub sid: &'a str,
    pub model: &'a str,
    pub effort: &'a str,
    pub permission_mode: &'a str,
    pub disable_slash_commands: bool,
    pub add_dirs: &'a [String],
    /// `--mcp-config` target (merged `.mcp.json` in profile mode, legacy
    /// `mcp.json` otherwise).
    pub mcp_config: &'a std::path::Path,
    /// `--settings` target (merged `settings.json` in profile mode, legacy
    /// `hooks.json` otherwise).
    pub settings: &'a std::path::Path,
    /// `--plugin-dir` (profile mode only).
    pub plugin_dir: Option<&'a std::path::Path>,
    /// `--agent <name>` to launch as the main agent (profile mode).
    pub agent: Option<&'a str>,
    /// `--bare` clean-room mode (spec §8.7), default off.
    pub bare: bool,
}

pub fn render_bootstrap(spec: &BootstrapSpec) -> String {
    let mut add_dirs: Vec<String> = if spec.add_dirs.is_empty() {
        vec![spec.cwd.to_string()]
    } else {
        spec.add_dirs.to_vec()
    };
    add_dirs.dedup();
    let add_args = add_dirs
        .iter()
        .map(|d| format!("--add-dir {}", shell_quote(d)))
        .collect::<Vec<_>>()
        .join(" ");
    let mut parts = vec![
        format!("cd {} &&", shell_quote(spec.cwd)),
        "exec claude".to_string(),
    ];
    if spec.bare {
        parts.push("--bare".to_string());
    }
    parts.push(format!("--session-id {}", spec.sid));
    if let Some(pd) = spec.plugin_dir {
        parts.push(format!("--plugin-dir {}", shell_quote(&pd.to_string_lossy())));
    }
    parts.push(format!(
        "--mcp-config {}",
        shell_quote(&spec.mcp_config.to_string_lossy())
    ));
    parts.push(format!(
        "--settings {}",
        shell_quote(&spec.settings.to_string_lossy())
    ));
    parts.push(format!(
        "--permission-mode {}",
        shell_quote(spec.permission_mode)
    ));
    parts.push("--permission-prompt-tool mcp__po-k__approve".to_string());
    parts.push(format!("--model {}", shell_quote(spec.model)));
    parts.push(format!("--effort {}", shell_quote(spec.effort)));
    if spec.disable_slash_commands {
        parts.push("--disable-slash-commands".to_string());
    }
    if !add_args.is_empty() {
        parts.push(add_args);
    }
    if let Some(a) = spec.agent {
        parts.push(format!("--agent {}", shell_quote(a)));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn renders_hooks_json_with_token_inline() {
        let s = render_hooks_json("http://127.0.0.1:7070", "abc-123", "TOK");
        assert!(s.contains("http://127.0.0.1:7070/sessions/abc-123/hooks/Stop"));
        assert!(s.contains("authorization: bearer TOK"));
        // The ingest handler's Json extractor 415s without this header.
        assert!(s.contains("content-type: application/json"));
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

    fn spec_for<'a>(
        cwd: &'a str,
        add_dirs: &'a [String],
        mcp: &'a std::path::Path,
        settings: &'a std::path::Path,
    ) -> BootstrapSpec<'a> {
        BootstrapSpec {
            cwd,
            sid: "abc-123",
            model: "sonnet",
            effort: "medium",
            permission_mode: "acceptEdits",
            disable_slash_commands: true,
            add_dirs,
            mcp_config: mcp,
            settings,
            plugin_dir: None,
            agent: None,
            bare: false,
        }
    }

    #[test]
    fn bootstrap_contains_all_required_flags() {
        let bash = render_bootstrap(&spec_for(
            "/workspace",
            &[],
            std::path::Path::new("/tmp/m.json"),
            std::path::Path::new("/tmp/h.json"),
        ));
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
        assert!(!bash.contains("--bare"));
        assert!(!bash.contains("--plugin-dir"));
        assert!(!bash.contains("--agent"));
    }

    #[test]
    fn bootstrap_profile_mode_flags() {
        let mut spec = spec_for(
            "/workspace",
            &[],
            std::path::Path::new("/tmp/.mcp.json"),
            std::path::Path::new("/tmp/settings.json"),
        );
        let pd = std::path::PathBuf::from("/tmp/plugin");
        spec.plugin_dir = Some(&pd);
        spec.agent = Some("security-reviewer");
        spec.bare = true;
        let bash = render_bootstrap(&spec);
        assert!(bash.starts_with("cd /workspace && exec claude --bare"));
        assert!(bash.contains("--plugin-dir /tmp/plugin"));
        assert!(bash.contains("--agent security-reviewer"));
    }

    #[test]
    fn bootstrap_quotes_paths_with_spaces() {
        let add = vec!["/home/me/with space".to_string()];
        let bash = render_bootstrap(&spec_for(
            "/home/me/with space",
            &add,
            std::path::Path::new("/tmp/m.json"),
            std::path::Path::new("/tmp/h.json"),
        ));
        assert!(bash.contains("'/home/me/with space'"));
    }

    fn sample_session(sid: &str, project: &str) -> RunningSession {
        RunningSession {
            sid: sid.into(),
            project: project.into(),
            cwd: "/workspace".into(),
            zellij_session: "po-k-po-k".into(),
            model: "opus".into(),
            effort: "xhigh".into(),
            started_at: "now".into(),
            hooks_path: "/h".into(),
            mcp_path: "/m".into(),
            pid: None,
            profiles: Vec::new(),
            plugin_dir: None,
        }
    }

    #[tokio::test]
    async fn registry_reports_sessions_per_project() {
        // The spawn() conflict guard keys off ids_for_project, so verify it
        // returns inserted sessions and stays empty for unknown projects.
        let reg = Registry::default();
        assert!(reg.ids_for_project("po-k").await.is_empty());
        reg.insert(sample_session("s1", "po-k")).await;
        assert_eq!(reg.ids_for_project("po-k").await, vec!["s1".to_string()]);
        assert!(reg.ids_for_project("other").await.is_empty());
        reg.remove("s1").await;
        assert!(reg.ids_for_project("po-k").await.is_empty());
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
