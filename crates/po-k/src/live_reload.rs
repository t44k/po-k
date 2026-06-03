//! Plugin-directory (re)generation for pushed profiles, and live hot-reload of
//! an active session's plugin when Xpo-k pushes a `profile_update` (M14 §3.6).

use anyhow::{Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::profile::{self, PokHookContext, Profile};
use crate::state::AppState;

/// Generate (or regenerate) a plugin directory for a pushed profile. Used by
/// `push_profile`. Returns the plugin dir path.
pub async fn generate_for(
    state: &AppState,
    session_id: Option<&str>,
    profile_json: &Value,
) -> Result<String> {
    let profile = Profile::from_json(profile_json)
        .map_err(|e| anyhow::anyhow!("invalid profile: {e}"))?;
    let sid = session_id
        .map(String::from)
        .unwrap_or_else(|| format!("preview-{}", Uuid::new_v4()));
    let session_dir = crate::config::expand_path(format!("~/.cache/po-k/sessions/{sid}"));
    std::fs::create_dir_all(&session_dir)
        .with_context(|| format!("creating {}", session_dir.display()))?;

    let cfg = state.config.read().await;
    let token_file = crate::config::expand_path(&cfg.auth.bearer_token_file);
    let base_url = cfg.hooks.base_url();
    drop(cfg);
    let pok = PokHookContext {
        base_url: &base_url,
        token: state.token.raw(),
        token_file: &token_file,
        sid: &sid,
    };
    let paths = profile::generate_plugin_dir(&session_dir, &profile, &pok)?;
    Ok(paths.dir.to_string_lossy().into_owned())
}

/// Apply a live profile update to a running session (M4.1): regenerate the
/// plugin files in place. CLAUDE.md + skills hot-reload automatically (CC
/// watches the files); agent/MCP/hook changes need a `/reload-plugins` nudge
/// sent into the pane.
pub async fn apply(state: &AppState, session_id: &str, profile_json: Value, changed_fields: Vec<String>) {
    let Some(running) = state.sessions.get(session_id).await else {
        tracing::warn!(session_id, "profile_update for unknown session");
        return;
    };
    if let Err(e) = regenerate_in_place(state, session_id, &profile_json).await {
        tracing::warn!(session_id, error = %e, "profile hot-reload failed");
        return;
    }
    // Structural changes (agents/mcp/hooks) require CC to reload plugins.
    let structural = changed_fields.iter().any(|f| {
        f.starts_with("agents") || f.starts_with("mcp_servers") || f.starts_with("hooks")
    });
    if structural {
        let _ = crate::zellij::submit_text(&running.zellij_session, "/reload-plugins").await;
        tracing::info!(session_id, "sent /reload-plugins after structural profile change");
    }
}

async fn regenerate_in_place(state: &AppState, sid: &str, profile_json: &Value) -> Result<()> {
    let profile = Profile::from_json(profile_json)
        .map_err(|e| anyhow::anyhow!("invalid profile: {e}"))?;
    let session_dir = crate::config::expand_path(format!("~/.cache/po-k/sessions/{sid}"));
    let cfg = state.config.read().await;
    let token_file = crate::config::expand_path(&cfg.auth.bearer_token_file);
    let base_url = cfg.hooks.base_url();
    drop(cfg);
    let pok = PokHookContext {
        base_url: &base_url,
        token: state.token.raw(),
        token_file: &token_file,
        sid,
    };
    profile::generate_plugin_dir(&session_dir, &profile, &pok)?;
    Ok(())
}
