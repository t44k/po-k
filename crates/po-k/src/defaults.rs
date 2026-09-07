//! Fixed defaults. po-k v2 has no per-box session configuration: everything a
//! session needs arrives in the create request, and these constants fill in
//! whatever the request leaves out. They are advertised by `GET /docs` so an
//! agent can see what it gets when it omits a field.

use std::path::PathBuf;
use std::time::Duration;

/// `--model` when the create request has none.
pub const MODEL: &str = "fable";
/// `--effort` when the create request has none.
pub const EFFORT: &str = "xhigh";
/// `--permission-mode` when the create request has none.
pub const PERMISSION_MODE: &str = "bypassPermissions";
/// Accepted values for `permission_mode` (CC's own list).
pub const PERMISSION_MODES: &[&str] = &[
    "default",
    "acceptEdits",
    "plan",
    "bypassPermissions",
    "dontAsk",
    "auto",
    "manual",
];
/// How long the `approve` MCP call blocks for an orchestrator decision before
/// po-k auto-denies.
pub const PERMISSION_TIMEOUT: Duration = Duration::from_secs(300);
/// CC is driven programmatically; slash commands typed by the model are noise.
pub const DISABLE_SLASH_COMMANDS: bool = true;
/// zellij session name = prefix + session name.
pub const ZELLIJ_SESSION_PREFIX: &str = "po-k-";
/// Per-session scratch (hooks.json, mcp.json, system_prompt.md).
pub const SESSIONS_DIR: &str = "~/.cache/po-k/sessions";
/// The events + hub SQLite database.
pub const EVENTS_DB: &str = "~/.config/po-k/events.db";
/// Default `server.bind`.
pub const BIND: &str = "0.0.0.0:13658";
/// Default port a hub uses to reach a remote po-k when the host has none.
pub const PORT: u16 = 13658;
/// Default DNS suffix appended to a bare host label by the hub (`ange` →
/// `ange.zrz`). Override with `POK_HOST_SUFFIX`.
pub const HOST_SUFFIX: &str = ".zrz";
/// Default `auth.bearer_token_file`.
pub const TOKEN_FILE: &str = "~/.config/po-k/auth.token";
/// Upper bound on `meta` JSON attached to hosts / watches (bytes).
pub const META_MAX_BYTES: usize = 2048;
/// A delivered notification that Hermes has not acknowledged within this time
/// is sent again (per-watch override: `ack_timeout_secs`).
pub const ACK_TIMEOUT: Duration = Duration::from_secs(900);
/// Replay interval doubles per attempt up to this cap.
pub const REPLAY_MAX_INTERVAL: Duration = Duration::from_secs(3600);
/// Deliveries + replays before a notification is parked as `failed`.
pub const REPLAY_MAX_ATTEMPTS: i64 = 24;
/// How often the deliverer looks for due work when nothing wakes it.
pub const DELIVERY_TICK: Duration = Duration::from_secs(15);
/// Bounds for a per-watch `ack_timeout_secs`.
pub const ACK_TIMEOUT_MIN_SECS: i64 = 30;
pub const ACK_TIMEOUT_MAX_SECS: i64 = 86_400;

pub fn session_dir(sid: &str) -> PathBuf {
    crate::config::expand_path(format!("{SESSIONS_DIR}/{sid}"))
}

pub fn host_suffix() -> String {
    std::env::var("POK_HOST_SUFFIX").unwrap_or_else(|_| HOST_SUFFIX.to_string())
}

pub fn remote_port() -> u16 {
    std::env::var("POK_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(PORT)
}
