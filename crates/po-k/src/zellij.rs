//! Zellij adapter. v1 shells out to the `zellij` CLI; M10.7 may upgrade to the
//! t44k/zellij@mcp-direct-ipc-refactor MCP socket once the protocol is pinned
//! down. The interface here is what the gateway needs:
//!   - `list_sessions` — names of running zellij sessions
//!   - `write_chars(session, text)` — paste text into the focused pane of the
//!     given session (we navigate to the right tab first via go-to-tab-name if
//!     a name is provided).

use anyhow::{Context, Result};
use std::process::Command;

#[derive(Debug, Clone)]
pub struct Session {
    pub name: String,
}

pub fn list_sessions() -> Result<Vec<Session>> {
    let out = Command::new("zellij")
        .arg("list-sessions")
        .arg("--short")
        .output()
        .context("spawning `zellij list-sessions` (is zellij on PATH?)")?;
    if !out.status.success() {
        // `list-sessions` exits 2 with "No active zellij sessions found." on a clean box.
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Ok(text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(|l| Session {
            name: l.to_string(),
        })
        .collect())
}

/// Write `text` into the focused pane of `session`. Trailing newline is the
/// caller's responsibility (omitting one is the way to *not* submit a CC prompt).
pub fn write_chars(session: &str, text: &str) -> Result<()> {
    let out = Command::new("zellij")
        .args(["--session", session, "action", "write-chars", text])
        .output()
        .context("spawning `zellij action write-chars`")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!("zellij write-chars failed: {}", stderr.trim());
    }
    Ok(())
}

/// Best-effort go-to-tab-name before a write, so multi-tab sessions can be
/// addressed. Silently ignores failures (the tab may not exist).
pub fn focus_tab(session: &str, tab_name: &str) {
    let _ = Command::new("zellij")
        .args(["--session", session, "action", "go-to-tab-name", tab_name])
        .output();
}
