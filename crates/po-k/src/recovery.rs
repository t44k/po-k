//! Rebuild the in-memory session view on startup.
//!
//! The `Registry` lives only in memory. After a restart every CC we spawned is
//! still running inside its zellij session, and its `hooks.json` / `mcp.json`
//! persist on disk with the bearer token and callback URL baked in, so CC
//! keeps calling back without any re-write.
//!
//! Recovery walks the DB for sessions believed alive (`ended_at IS NULL`),
//! confirms the zellij session and MCP socket really are up, re-inserts them
//! into the `Registry`, and restarts the JSONL tailer (which resumes from a
//! stored byte offset).
//!
//!
//! Sessions whose zellij is gone are marked ended (`cc_lost`).

use anyhow::Result;
use serde_json::json;

use crate::defaults;
use crate::events_store;
use crate::jsonl_tail;
use crate::session::{self, RunningSession};
use crate::state::AppState;
use crate::zellij;

pub async fn recover_sessions(state: &AppState) -> Result<()> {
    let unended = events_store::unended_sessions(&state.db).await?;
    if unended.is_empty() {
        return Ok(());
    }
    let live = zellij::list_sessions().await.unwrap_or_default();
    let expected_url = state.config.server.callback_base_url();
    let mut recovered = 0usize;
    let mut lost = 0usize;
    for row in unended {
        let zname = row.zellij_session.clone();
        // `list-sessions --short` includes EXITED zombies; the socket probe is
        // what tells us the session is *actually* serving requests.
        let listed = live.iter().any(|s| s == &zname);
        let alive = listed && zellij::is_socket_alive(&zname).await;
        if !alive {
            let ts = events_store::now_iso();
            let _ = events_store::mark_session_ended(&state.db, &row.sid, &ts).await;
            let _ = session::append_lifecycle_event(
                state,
                &row.sid,
                "cc_lost",
                &json!({ "reason": "zellij not alive at recovery", "zellij_session": zname }),
            )
            .await;
            lost += 1;
            tracing::info!(sid = %row.sid, zellij_session = %zname, "session ended at recovery");
            continue;
        }

        // hooks.json / mcp.json paths derive from the sid. v1 profile sessions
        // kept them under a generated plugin dir; v2 sessions keep them flat.
        let dir = defaults::session_dir(&row.sid);
        let (hooks_path, mcp_path) = match row.plugin_dir.as_deref() {
            Some(pd) => {
                let pd = std::path::Path::new(pd);
                (pd.join("hooks").join("hooks.json"), pd.join(".mcp.json"))
            }
            None => (dir.join("hooks.json"), dir.join("mcp.json")),
        };
        warn_if_callback_url_changed(&row.sid, &hooks_path, &expected_url);

        let parse_list = |s: Option<&str>| -> Vec<String> {
            s.and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default()
        };
        let running = RunningSession {
            sid: row.sid.clone(),
            name: row.name.clone(),
            cwd: row.cwd.clone(),
            zellij_session: zname,
            model: row.model.clone().unwrap_or_else(|| defaults::MODEL.to_string()),
            effort: row.effort.clone().unwrap_or_else(|| defaults::EFFORT.to_string()),
            permission_mode: row
                .permission_mode
                .clone()
                .unwrap_or_else(|| defaults::PERMISSION_MODE.to_string()),
            agent: row.agent.clone(),
            plugins: parse_list(row.plugins.as_deref()),
            mcp_servers: parse_list(row.mcp_servers.as_deref()),
            started_at: row.started_at.clone(),
            hooks_path: hooks_path.to_string_lossy().into_owned(),
            mcp_path: mcp_path.to_string_lossy().into_owned(),
            pid: row.pid,
        };
        state.sessions.insert(running).await;
        let _ = session::append_lifecycle_event(state, &row.sid, "cc_recovered", &json!({})).await;
        // Restart the tailer — it resumes from `sessions.last_jsonl_offset`.
        jsonl_tail::spawn(state.clone(), row.sid.clone(), row.cwd.clone());
        recovered += 1;
        tracing::info!(sid = %row.sid, name = %row.name, "session recovered");
    }
    tracing::info!(recovered, lost, "session recovery complete");
    Ok(())
}

/// CC keeps curling the URL baked into hooks.json at spawn time. If the bind
/// changed since (e.g. a port move), those curls go nowhere and the session's
/// status freezes — say so loudly.
fn warn_if_callback_url_changed(sid: &str, hooks_path: &std::path::Path, expected: &str) {
    let Ok(text) = std::fs::read_to_string(hooks_path) else {
        return;
    };
    if !text.contains(&format!("{expected}/sessions/")) {
        tracing::warn!(
            sid,
            expected_callback = expected,
            hooks = %hooks_path.display(),
            "recovered session's hooks.json points at a different po-k URL; its hooks will not reach this server — recreate the session"
        );
    }
}
