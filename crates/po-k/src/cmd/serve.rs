//! `po-k serve` — the HTTP service + hub.
//!
//!   1. load config (missing = defaults) + the bearer token (`POK_TOKEN` seeds the file),
//!      and verify the zellij on PATH is the MCP fork with a working socket,
//!   2. open events.db, recover surviving sessions, respawn hub watchers,
//!   3. serve the API on `server.bind` until SIGINT/SIGTERM.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;

use crate::auth::{self, Token};
use crate::config;
use crate::defaults;
use crate::events_store;
use crate::http;
use crate::state::AppState;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Config file (default ~/.config/po-k/po-k.yaml; missing = defaults).
    #[arg(long, env = "POK_CONFIG")]
    pub config: Option<PathBuf>,
    /// Bind override, e.g. `127.0.0.1:7071`.
    #[arg(long, env = "POK_BIND")]
    pub bind: Option<String>,
    /// Bearer token file override.
    #[arg(long, env = "POK_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
    /// Events + hub database (default ~/.config/po-k/events.db).
    #[arg(long, env = "POK_DB")]
    pub db: Option<PathBuf>,
    /// Bearer token value: written to the token file (0600) at startup.
    #[arg(long, env = "POK_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
    /// Kept for symmetry — `po-k serve` always runs in the foreground.
    #[arg(long)]
    pub foreground: bool,
    /// Install + enable a systemd unit (user unit by default), then exit.
    #[arg(long)]
    pub install_systemd: bool,
    /// Write a system unit at /etc/systemd/system/po-k.service (requires root).
    #[arg(long)]
    pub system: bool,
}

pub async fn run(args: Args) -> Result<()> {
    if args.install_systemd || args.system {
        return crate::systemd_install::install(!args.system);
    }

    let cfg_path = args.config.clone().unwrap_or_else(config::default_config_path);
    let mut cfg = config::load_from(&cfg_path).with_context(|| format!("loading {}", cfg_path.display()))?;
    if let Some(b) = &args.bind {
        cfg.server.bind = b.clone();
    }
    if let Some(tf) = &args.token_file {
        cfg.auth.bearer_token_file = tf.to_string_lossy().into_owned();
    }
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    if let Some(t) = args.token.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        auth::write_token_file(&token_path, t)?;
        tracing::info!(path = %token_path.display(), "token file seeded from POK_TOKEN");
    } else if !token_path.exists() {
        auth::write_token_file(&token_path, &auth::generate_hex_token())?;
        tracing::warn!(path = %token_path.display(), "no token file — generated one; share it with the hub (or set POK_TOKEN to the fleet key)");
    }
    let token = Token::from_file(&token_path).with_context(|| format!("loading bearer token from {}", token_path.display()))?;

    // Refuse to start without a working zellij MCP: nothing could be driven.
    crate::zellij::preflight().await?;

    let addr = cfg.server.socket_addr()?;
    let callback = cfg.server.callback_base_url();
    let db_path = args.db.clone().unwrap_or_else(|| config::expand_path(defaults::EVENTS_DB));
    let db = events_store::open(&db_path).await.with_context(|| format!("opening events db at {}", db_path.display()))?;
    tracing::info!(path = %db_path.display(), "events.db ready");

    let state = AppState::new(token, cfg, db);

    // Rebuild the registry from the DB before serving, so /sessions and
    // /status reflect surviving CC processes immediately.
    if let Err(e) = crate::recovery::recover_sessions(&state).await {
        tracing::warn!(error = %e, "session recovery failed; starting clean");
    }
    crate::hub::watcher::respawn_all(&state).await;
    // Deliver anything pending/overdue and keep doing so; wakes on new rows.
    crate::hub::deliver::spawn(state.clone());

    let listener = tokio::net::TcpListener::bind(&addr).await.with_context(|| format!("binding {addr}"))?;
    tracing::info!(
        %addr,
        callback = %callback,
        version = env!("CARGO_PKG_VERSION"),
        config = %cfg_path.display(),
        "po-k serve listening"
    );
    axum::serve(listener, http::router(state).into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;
    Ok(())
}

/// How long in-flight requests get after SIGTERM/SIGINT before the process
/// exits regardless. Long-polls (`/wait`, `/events?wait=`) are cheap for the
/// caller to retry; a restart must not wait minutes for them.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    let term = async {
        if let Ok(mut s) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!(grace = ?SHUTDOWN_GRACE, "shutdown signal received; draining");
    tokio::spawn(async {
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        tracing::warn!("shutdown grace elapsed; exiting with connections still open");
        std::process::exit(0);
    });
}
