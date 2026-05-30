//! `po-k serve` — the HTTP service.
//!
//! Loads `po-k.yaml` + the bearer token, builds the shared `AppState`, optionally
//! spawns the hot-reload watcher, and runs the axum router until SIGINT/SIGTERM.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::auth::Token;
use crate::config;
use crate::config_watch;
use crate::events_store;
use crate::http;
use crate::state::AppState;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Bind override (e.g. `0.0.0.0:7070`). Logs a one-time WARN if non-loopback.
    #[arg(long)]
    pub bind: Option<String>,
    /// Kept for symmetry — `po-k serve` always runs in the foreground today.
    #[arg(long)]
    pub foreground: bool,
    /// Install + enable a systemd unit (user unit by default), then exit.
    #[arg(long)]
    pub install_systemd: bool,
    /// Write a system unit at /etc/systemd/system/po-k.service (requires root).
    /// Implies --install-systemd. Default: user unit.
    #[arg(long)]
    pub system: bool,
}

pub async fn run(args: Args) -> Result<()> {
    if args.install_systemd || args.system {
        return crate::systemd_install::install(!args.system);
    }

    let cfg_path = config::default_config_path();
    let cfg = config::load_from(&cfg_path)
        .with_context(|| format!("loading {} (did you run `po-k init`?)", cfg_path.display()))?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    let token = Token::from_file(&token_path)
        .with_context(|| format!("loading bearer token from {}", token_path.display()))?;

    let bind = args.bind.clone().unwrap_or_else(|| cfg.server.bind.clone());
    let reload_on_change = cfg.server.reload_on_change;
    let addr: SocketAddr = SocketAddr::from_str(&bind)
        .with_context(|| format!("parsing bind address {bind:?}"))?;
    if !is_loopback(&addr) {
        tracing::warn!(%addr, "binding non-loopback address — make sure you tunnel this (SSH/Tailscale/WireGuard); po-k does not terminate TLS");
    }

    let db_path = config::expand_path("~/.config/po-k/events.db");
    let db = events_store::open(&db_path)
        .await
        .with_context(|| format!("opening events db at {}", db_path.display()))?;
    tracing::info!(path = %db_path.display(), "events.db ready");

    let state = AppState::new(token, cfg, cfg_path.clone(), db);

    // Rebuild the Registry from the DB before serving requests, so /sessions
    // and /status reflect surviving CC subprocesses immediately on first GET.
    // Best-effort: a failure here logs and we keep serving (empty registry).
    if let Err(e) = crate::recovery::recover_sessions(&state).await {
        tracing::warn!(error = %e, "session recovery failed; starting clean");
    }

    if reload_on_change {
        config_watch::spawn(cfg_path.clone(), state.config.clone());
    }

    let app = http::router(state);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, version = env!("CARGO_PKG_VERSION"), config = %cfg_path.display(), "po-k serve listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;
    Ok(())
}

fn is_loopback(addr: &SocketAddr) -> bool {
    match addr {
        SocketAddr::V4(v4) => v4.ip().is_loopback(),
        SocketAddr::V6(v6) => v6.ip().is_loopback(),
    }
}

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
    tracing::info!("shutdown signal received");
}
