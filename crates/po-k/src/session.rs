//! Session lifecycle: spawn CC inside a per-session zellij session, track it in
//! memory + in the events.db `sessions` table, tear it down on delete.
//!
//! A session is fully described by its create request (cwd, model, plugins,
//! MCP servers, …); nothing is configured on the box. po-k writes two small
//! files per session under `~/.cache/po-k/sessions/<sid>/`:
//!   - `hooks.json` — passed via `--settings`: po-k's lifecycle hook curls +
//!     the settings keys po-k owns.
//!   - `mcp.json`   — passed via `--mcp-config`: the request's MCP servers plus
//!     po-k's own permission server (`po-k cc-mcp`), always last.
//!
//! and optionally `system_prompt.md` for `--append-system-prompt-file`.

use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::defaults;
use crate::events_store::{self, SessionRow};
use crate::profile::McpServer;
use crate::state::AppState;
use crate::zellij;

#[derive(Debug, Clone, Serialize)]
pub struct RunningSession {
    pub sid: String,
    pub name: String,
    pub cwd: String,
    pub zellij_session: String,
    pub model: String,
    pub effort: String,
    pub permission_mode: String,
    pub agent: Option<String>,
    pub plugins: Vec<String>,
    /// Names of the extra MCP servers the request supplied.
    pub mcp_servers: Vec<String>,
    pub started_at: String,
    pub hooks_path: String,
    pub mcp_path: String,
    /// Resolved asynchronously after CC starts. Always None today.
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

    pub async fn ids_for_name(&self, name: &str) -> Vec<String> {
        self.inner
            .lock()
            .await
            .values()
            .filter(|s| s.name == name)
            .map(|s| s.sid.clone())
            .collect()
    }

    pub async fn insert(&self, s: RunningSession) {
        self.inner.lock().await.insert(s.sid.clone(), s);
    }

    pub(crate) async fn remove(&self, sid: &str) -> Option<RunningSession> {
        self.inner.lock().await.remove(sid)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("a session named {name:?} is already running (sid {sid})")]
    AlreadyRunning { name: String, sid: String },
    #[error("spawning session: {0}")]
    Other(#[from] anyhow::Error),
}

/// Validated create inputs (see `core::sessions::validate`).
#[derive(Debug, Default, Clone)]
pub struct SpawnRequest {
    pub cwd: String,
    pub name: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub permission_mode: Option<String>,
    pub plugins: Vec<String>,
    pub mcp_servers: IndexMap<String, McpServer>,
    pub agent: Option<String>,
    pub add_dirs: Vec<String>,
    pub system_prompt: Option<String>,
}

/// Directory basename → session name (`[A-Za-z0-9_-]`, else `-`).
pub fn slugify(raw: &str) -> String {
    let s: String = raw
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let trimmed = s.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "session".to_string()
    } else {
        trimmed
    }
}

pub fn name_for(cwd: &str, explicit: Option<&str>) -> String {
    match explicit {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => slugify(
            &Path::new(cwd)
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default(),
        ),
    }
}

pub fn zellij_session_name(name: &str) -> String {
    format!("{}{}", defaults::ZELLIJ_SESSION_PREFIX, name)
}

pub async fn spawn(state: &AppState, req: SpawnRequest) -> Result<RunningSession, SpawnError> {
    let name = name_for(&req.cwd, req.name.as_deref());

    let cwd_path = Path::new(&req.cwd);
    if !cwd_path.exists() {
        std::fs::create_dir_all(cwd_path)
            .map_err(|e| SpawnError::Other(anyhow::anyhow!("creating directory {:?}: {e}", req.cwd)))?;
        tracing::info!(cwd = %req.cwd, "created session directory");
    }

    // One CC per name: every session with a name shares one zellij session
    // (and pane), so a second spawn would type its bootstrap into the pane
    // already running the first CC. Refuse instead of clobbering.
    if let Some(sid) = state.sessions.ids_for_name(&name).await.into_iter().next() {
        return Err(SpawnError::AlreadyRunning { name, sid });
    }
    spawn_inner(state, name, req).await.map_err(SpawnError::Other)
}

async fn spawn_inner(state: &AppState, name: String, req: SpawnRequest) -> Result<RunningSession> {
    let sid = Uuid::new_v4().to_string();
    let zellij_session = zellij_session_name(&name);
    let session_dir = defaults::session_dir(&sid);
    std::fs::create_dir_all(&session_dir)
        .with_context(|| format!("creating {}", session_dir.display()))?;

    let token_file = crate::config::expand_path(&state.config.auth.bearer_token_file);
    let base_url = state.config.server.callback_base_url();

    let model = req.model.clone().unwrap_or_else(|| defaults::MODEL.to_string());
    let effort = req.effort.clone().unwrap_or_else(|| defaults::EFFORT.to_string());
    let permission_mode = req
        .permission_mode
        .clone()
        .unwrap_or_else(|| defaults::PERMISSION_MODE.to_string());

    let hooks_path = session_dir.join("hooks.json");
    std::fs::write(&hooks_path, render_settings_json(&base_url, &sid, state.token.raw()))
        .with_context(|| format!("writing {}", hooks_path.display()))?;
    let mcp_path = session_dir.join("mcp.json");
    let pok_bin = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "po-k".to_string());
    std::fs::write(&mcp_path, render_mcp_json(&sid, &base_url, &token_file, &pok_bin, &req.mcp_servers))
        .with_context(|| format!("writing {}", mcp_path.display()))?;
    let system_prompt_path = match &req.system_prompt {
        Some(text) if !text.trim().is_empty() => {
            let p = session_dir.join("system_prompt.md");
            std::fs::write(&p, text).with_context(|| format!("writing {}", p.display()))?;
            Some(p)
        }
        _ => None,
    };

    let (plugin_dirs, plugin_urls): (Vec<String>, Vec<String>) = req
        .plugins
        .iter()
        .cloned()
        .partition(|p| !(p.starts_with("http://") || p.starts_with("https://")));

    // CC's first-run "trust this folder?" dialog cannot be answered in a po-k
    // pane; record the trust up front (see `cc_trust`).
    match crate::cc_trust::ensure_trusted(&req.cwd) {
        Ok(true) => tracing::info!(cwd = %req.cwd, "marked directory trusted in ~/.claude.json"),
        Ok(false) => {}
        Err(e) => tracing::warn!(cwd = %req.cwd, error = %e, "could not pre-trust directory; CC may show its trust dialog"),
    }

    zellij::ensure_session(&zellij_session)
        .await
        .with_context(|| format!("ensuring zellij session {zellij_session:?}"))?;

    let spec = BootstrapSpec {
        cwd: &req.cwd,
        sid: &sid,
        model: &model,
        effort: &effort,
        permission_mode: &permission_mode,
        disable_slash_commands: defaults::DISABLE_SLASH_COMMANDS,
        add_dirs: &req.add_dirs,
        mcp_config: &mcp_path,
        settings: &hooks_path,
        plugin_dirs: &plugin_dirs,
        plugin_urls: &plugin_urls,
        agent: req.agent.as_deref(),
        system_prompt_file: system_prompt_path.as_deref(),
    };
    let cmd = render_bootstrap(&spec);
    tracing::info!(sid = %sid, zellij = %zellij_session, cmd = %cmd, "bootstrapping CC in pane");
    zellij::write_to_focused_pane(&zellij_session, &format!("{cmd}\n")).await?;

    let started_at = events_store::now_iso();
    let mcp_names: Vec<String> = req.mcp_servers.keys().cloned().collect();
    let row = SessionRow {
        sid: sid.clone(),
        name: name.clone(),
        cwd: req.cwd.clone(),
        zellij_session: zellij_session.clone(),
        model: Some(model.clone()),
        effort: Some(effort.clone()),
        started_at: started_at.clone(),
        ended_at: None,
        pid: None,
        last_event_seq: 0,
        plugin_dir: None,
        plugins: serde_json::to_string(&req.plugins).ok(),
        mcp_servers: serde_json::to_string(&mcp_names).ok(),
        permission_mode: Some(permission_mode.clone()),
        agent: req.agent.clone(),
    };
    events_store::insert_session(&state.db, &row).await?;

    append_lifecycle_event(
        state,
        &sid,
        "cc_started",
        &json!({
            "name": name,
            "model": model,
            "effort": effort,
            "permission_mode": permission_mode,
            "cwd": req.cwd,
            "zellij_session": zellij_session,
            "plugins": req.plugins,
            "mcp_servers": mcp_names,
            "agent": req.agent,
        }),
    )
    .await?;

    let running = RunningSession {
        sid: sid.clone(),
        name,
        cwd: req.cwd.clone(),
        zellij_session,
        model,
        effort,
        permission_mode,
        agent: req.agent,
        plugins: req.plugins,
        mcp_servers: mcp_names,
        started_at,
        hooks_path: hooks_path.to_string_lossy().into_owned(),
        mcp_path: mcp_path.to_string_lossy().into_owned(),
        pid: None,
    };
    state.sessions.insert(running.clone()).await;

    // Per-session JSONL tailer projects CC's transcript into events rows. It
    // waits for the transcript as long as the session is alive.
    crate::jsonl_tail::spawn(state.clone(), sid, req.cwd);

    Ok(running)
}

pub async fn kill(state: &AppState, sid: &str) -> Result<()> {
    let running = state
        .sessions
        .get(sid)
        .await
        .ok_or_else(|| anyhow::anyhow!("session {sid} not found"))?;

    // Graceful exit first: /exit into the pane.
    let _ = zellij::write_to_focused_pane(&running.zellij_session, "/exit\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Always reap the zellij session so a future start re-creates it cleanly.
    let _ = zellij::kill_session(&running.zellij_session).await;
    let _ = std::fs::remove_dir_all(defaults::session_dir(sid));

    state.sessions.remove(sid).await;
    let ts = events_store::now_iso();
    let _ = append_lifecycle_event(state, sid, "cc_exited", &json!({})).await;
    events_store::mark_session_ended(&state.db, sid, &ts).await?;
    state.bus.drop_session(sid).await;
    Ok(())
}

/// Append a lifecycle event through the central `core::events::record` choke
/// point (DB + bus wake).
pub(crate) async fn append_lifecycle_event(
    state: &AppState,
    sid: &str,
    kind: &str,
    payload: &Value,
) -> Result<()> {
    crate::core::events::record(state, sid, kind, payload).await?;
    Ok(())
}

/// CC settings key po-k owns on every settings file it generates. po-k drives
/// CC non-interactively, so CC's interactive dangerous-mode confirmation would
/// stall the session at a prompt no orchestrator can answer.
const SKIP_DANGEROUS_PROMPT: &str = "skipDangerousModePermissionPrompt";

/// The `--settings` file: po-k's lifecycle hooks plus the settings keys po-k
/// owns. Written as `hooks.json` in the session dir.
pub fn render_settings_json(base_url: &str, sid: &str, token: &str) -> String {
    let mut settings = serde_json::Map::new();
    settings.insert("hooks".into(), hooks_block(base_url, sid, token));
    settings.insert(SKIP_DANGEROUS_PROMPT.into(), json!(true));
    serde_json::to_string_pretty(&Value::Object(settings)).expect("settings serialize")
}

fn hooks_block(base_url: &str, sid: &str, token: &str) -> Value {
    let mk = |event: &str| -> Value {
        let url = format!("{base_url}/sessions/{sid}/hooks/{event}");
        // The bearer is required (every route is protected) and
        // `content-type: application/json` keeps the ingest body parsing
        // unambiguous.
        let command = format!(
            "curl -sX POST '{url}' -H 'authorization: bearer {token}' -H 'content-type: application/json' --data-binary @-",
        );
        json!({ "matcher": "", "hooks": [{ "type": "command", "command": command }] })
    };
    json!({
        "UserPromptSubmit": [mk("UserPromptSubmit")],
        "Stop":             [mk("Stop")],
        "SubagentStop":     [mk("SubagentStop")],
        "PostToolUse":      [mk("PostToolUse")],
        "Notification":     [mk("Notification")],
        "SessionEnd":       [mk("SessionEnd")],
    })
}

/// The `--mcp-config` file: the request's servers, then po-k's permission
/// server inserted last so a request can never override it. `pok_bin` is the
/// absolute path of this very binary, so the shim CC launches is the same
/// build as the server regardless of `PATH` inside the pane.
pub fn render_mcp_json(
    sid: &str,
    base_url: &str,
    token_file: &Path,
    pok_bin: &str,
    extra: &IndexMap<String, McpServer>,
) -> String {
    let mut servers = serde_json::Map::new();
    for (name, s) in extra {
        if name == "po-k" {
            continue;
        }
        servers.insert(name.clone(), crate::profile::mcp_server_json(s, false));
    }
    servers.insert(
        "po-k".into(),
        json!({
            "command": pok_bin,
            "args": [
                "cc-mcp",
                "--session-id", sid,
                "--base-url", base_url,
                "--token-file", token_file.to_string_lossy().into_owned(),
            ]
        }),
    );
    serde_json::to_string_pretty(&json!({ "mcpServers": Value::Object(servers) }))
        .expect("mcp.json serialize")
}

/// Everything needed to render the `claude` bootstrap command line.
pub struct BootstrapSpec<'a> {
    pub cwd: &'a str,
    pub sid: &'a str,
    pub model: &'a str,
    pub effort: &'a str,
    pub permission_mode: &'a str,
    pub disable_slash_commands: bool,
    /// Extra `--add-dir`s; `cwd` is always included first.
    pub add_dirs: &'a [String],
    pub mcp_config: &'a Path,
    pub settings: &'a Path,
    pub plugin_dirs: &'a [String],
    pub plugin_urls: &'a [String],
    pub agent: Option<&'a str>,
    pub system_prompt_file: Option<&'a Path>,
}

pub fn render_bootstrap(spec: &BootstrapSpec) -> String {
    let mut add_dirs: Vec<&str> = vec![spec.cwd];
    for d in spec.add_dirs {
        if !add_dirs.contains(&d.as_str()) {
            add_dirs.push(d);
        }
    }
    let mut parts = vec![format!("cd {} &&", shell_quote(spec.cwd)), "exec claude".to_string()];
    parts.push(format!("--session-id {}", spec.sid));
    for pd in spec.plugin_dirs {
        parts.push(format!("--plugin-dir {}", shell_quote(pd)));
    }
    for pu in spec.plugin_urls {
        parts.push(format!("--plugin-url {}", shell_quote(pu)));
    }
    parts.push(format!("--mcp-config {}", shell_quote(&spec.mcp_config.to_string_lossy())));
    parts.push(format!("--settings {}", shell_quote(&spec.settings.to_string_lossy())));
    parts.push(format!("--permission-mode {}", shell_quote(spec.permission_mode)));
    parts.push("--permission-prompt-tool mcp__po-k__approve".to_string());
    parts.push(format!("--model {}", shell_quote(spec.model)));
    parts.push(format!("--effort {}", shell_quote(spec.effort)));
    if spec.disable_slash_commands {
        parts.push("--disable-slash-commands".to_string());
    }
    for d in add_dirs {
        parts.push(format!("--add-dir {}", shell_quote(d)));
    }
    if let Some(p) = spec.system_prompt_file {
        parts.push(format!("--append-system-prompt-file {}", shell_quote(&p.to_string_lossy())));
    }
    if let Some(a) = spec.agent {
        parts.push(format!("--agent {}", shell_quote(a)));
    }
    parts.join(" ")
}

/// Minimal POSIX single-quote shell escaper.
pub fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | ','))
    {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_json_has_hooks_token_and_skips_dangerous_prompt() {
        let s = render_settings_json("http://127.0.0.1:13658", "abc-123", "TOK");
        assert!(s.contains("http://127.0.0.1:13658/sessions/abc-123/hooks/Stop"));
        assert!(s.contains("authorization: bearer TOK"));
        assert!(s.contains("content-type: application/json"));
        for event in ["UserPromptSubmit", "Stop", "SubagentStop", "PostToolUse", "Notification", "SessionEnd"] {
            assert!(s.contains(event), "missing event {event}");
        }
        let body: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(body["skipDangerousModePermissionPrompt"], true);
        assert!(body["hooks"]["Stop"].is_array());
    }

    #[test]
    fn mcp_json_puts_pok_last_and_cannot_be_clobbered() {
        let mut extra = IndexMap::new();
        extra.insert(
            "linear".to_string(),
            McpServer { command: Some("node".into()), args: vec!["x.js".into()], ..Default::default() },
        );
        extra.insert("po-k".to_string(), McpServer { command: Some("evil".into()), ..Default::default() });
        let s = render_mcp_json("abc-123", "http://127.0.0.1:13658", Path::new("/home/me/.config/po-k/auth.token"), "/usr/local/bin/po-k", &extra);
        let v: Value = serde_json::from_str(&s).unwrap();
        let servers = v["mcpServers"].as_object().unwrap();
        assert_eq!(servers.keys().collect::<Vec<_>>(), vec!["linear", "po-k"]);
        assert_eq!(servers["po-k"]["command"], "/usr/local/bin/po-k");
        assert_eq!(servers["po-k"]["args"][0], "cc-mcp");
        assert!(s.contains("/home/me/.config/po-k/auth.token"));
        assert_eq!(servers["linear"]["command"], "node");
    }

    fn spec<'a>(cwd: &'a str, add_dirs: &'a [String]) -> BootstrapSpec<'a> {
        BootstrapSpec {
            cwd,
            sid: "abc-123",
            model: "sonnet",
            effort: "medium",
            permission_mode: "bypassPermissions",
            disable_slash_commands: true,
            add_dirs,
            mcp_config: Path::new("/tmp/m.json"),
            settings: Path::new("/tmp/h.json"),
            plugin_dirs: &[],
            plugin_urls: &[],
            agent: None,
            system_prompt_file: None,
        }
    }

    #[test]
    fn bootstrap_contains_all_required_flags() {
        let bash = render_bootstrap(&spec("/workspace", &[]));
        assert!(bash.starts_with("cd /workspace && exec claude"));
        assert!(bash.contains("--session-id abc-123"));
        assert!(bash.contains("--model sonnet"));
        assert!(bash.contains("--effort medium"));
        assert!(bash.contains("--permission-mode bypassPermissions"));
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
    fn bootstrap_repeats_plugin_flags_and_system_prompt() {
        let dirs = vec!["/zirzen/base/plugins/sapi".to_string(), "/tmp/p.zip".to_string()];
        let urls = vec!["https://example.com/x.zip".to_string()];
        let add = vec!["/other".to_string(), "/workspace".to_string()];
        let mut s = spec("/workspace", &add);
        s.plugin_dirs = &dirs;
        s.plugin_urls = &urls;
        s.agent = Some("lead-reviewer");
        s.system_prompt_file = Some(Path::new("/tmp/sp.md"));
        let bash = render_bootstrap(&s);
        assert!(bash.contains("--plugin-dir /zirzen/base/plugins/sapi --plugin-dir /tmp/p.zip"));
        assert!(bash.contains("--plugin-url https://example.com/x.zip"));
        assert!(bash.contains("--append-system-prompt-file /tmp/sp.md"));
        assert!(bash.contains("--agent lead-reviewer"));
        // cwd first, extra dirs after, duplicates dropped.
        assert_eq!(bash.matches("--add-dir").count(), 2);
        assert!(bash.contains("--add-dir /workspace --add-dir /other"));
    }

    #[test]
    fn bootstrap_quotes_paths_with_spaces() {
        let bash = render_bootstrap(&spec("/home/me/with space", &[]));
        assert!(bash.contains("'/home/me/with space'"));
    }

    #[test]
    fn slugify_and_name_for() {
        assert_eq!(slugify("My Project.v2"), "My-Project-v2");
        assert_eq!(slugify("///"), "session");
        assert_eq!(name_for("/workspace", None), "workspace");
        assert_eq!(name_for("/a/b/src", Some("api")), "api");
        assert_eq!(name_for("/a/b/src", Some("")), "src");
        assert_eq!(zellij_session_name("api"), "po-k-api");
    }

    fn sample_session(sid: &str, name: &str) -> RunningSession {
        RunningSession {
            sid: sid.into(),
            name: name.into(),
            cwd: "/workspace".into(),
            zellij_session: "po-k-x".into(),
            model: "opus".into(),
            effort: "xhigh".into(),
            permission_mode: "bypassPermissions".into(),
            agent: None,
            plugins: Vec::new(),
            mcp_servers: Vec::new(),
            started_at: "now".into(),
            hooks_path: "/h".into(),
            mcp_path: "/m".into(),
            pid: None,
        }
    }

    #[tokio::test]
    async fn registry_reports_sessions_per_name() {
        let reg = Registry::default();
        assert!(reg.ids_for_name("po-k").await.is_empty());
        reg.insert(sample_session("s1", "po-k")).await;
        assert_eq!(reg.ids_for_name("po-k").await, vec!["s1".to_string()]);
        assert!(reg.ids_for_name("other").await.is_empty());
        reg.remove("s1").await;
        assert!(reg.ids_for_name("po-k").await.is_empty());
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
