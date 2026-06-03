//! Rebuild the in-memory session view on startup.
//!
//! po-k's `Registry` lives only in memory. After a restart, every CC
//! subprocess we previously spawned is still running inside its zellij
//! session — the per-session `hooks.json` and `mcp.json` persist on disk with
//! the (stable) bearer token baked in, and `base_url` comes from the config —
//! so CC keeps firing hooks at the new server without any re-write. All we
//! need to do is: walk the DB for sessions we believed alive
//! (`ended_at IS NULL`), confirm the zellij session + MCP socket are *actually*
//! still up, re-insert into `Registry`, and restart the JSONL tailer for each
//! (which now resumes from a stored byte offset, so no events are lost or
//! duplicated).
//!
//! Sessions whose zellij is gone are marked ended.

use anyhow::Result;
use serde_json::json;

use crate::config;
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
    let cfg = state.config.read().await.clone();
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

        // hooks.json / mcp.json paths are derivable from sid — they're not
        // persisted in the DB but always live under ~/.cache/po-k/sessions/<sid>/.
        // Profile sessions (M14) keep them under plugin/; legacy sessions keep
        // them flat in the session dir.
        let dir = config::expand_path(format!("~/.cache/po-k/sessions/{}", row.sid));
        let (hooks_path, mcp_path) = match row.plugin_dir.as_deref() {
            Some(pd) => {
                let pd = std::path::Path::new(pd);
                (
                    pd.join("hooks").join("hooks.json"),
                    pd.join(".mcp.json"),
                )
            }
            None => (dir.join("hooks.json"), dir.join("mcp.json")),
        };
        let profiles: Vec<String> = row
            .profiles
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let running = RunningSession {
            sid: row.sid.clone(),
            project: row.project.clone(),
            cwd: row.cwd.clone(),
            zellij_session: zname,
            model: row.model.clone().unwrap_or_else(|| cfg.cc.model.clone()),
            effort: row.effort.clone().unwrap_or_else(|| cfg.cc.effort.clone()),
            started_at: row.started_at.clone(),
            hooks_path: hooks_path.to_string_lossy().into_owned(),
            mcp_path: mcp_path.to_string_lossy().into_owned(),
            pid: row.pid,
            profiles,
            plugin_dir: row.plugin_dir.clone(),
        };
        state.sessions.insert(running).await;
        let _ = session::append_lifecycle_event(state, &row.sid, "cc_recovered", &json!({})).await;
        // Restart the tailer — it resumes from `sessions.last_jsonl_offset`.
        jsonl_tail::spawn(state.clone(), row.sid.clone(), row.cwd.clone());
        recovered += 1;
        tracing::info!(sid = %row.sid, project = %row.project, "session recovered");
    }
    tracing::info!(recovered, lost, "session recovery complete");
    Ok(())
}
