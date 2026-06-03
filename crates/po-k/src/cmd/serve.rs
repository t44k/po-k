//! `po-k serve` — headless WebSocket client + CC manager (M14 §5.9).
//!
//! po-k no longer runs an orchestrator-facing HTTP server. It:
//!   1. loads config + bearer token,
//!   2. opens the events DB,
//!   3. recovers surviving sessions,
//!   4. starts the localhost-only hook listener (CC callbacks),
//!   5. connects to Xpo-k over WebSocket and registers,
//!   6. runs until SIGINT/SIGTERM.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;

use crate::auth::Token;
use crate::config;
use crate::config_watch;
use crate::events_store;
use crate::hook_listener;
use crate::state::AppState;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Kept for symmetry — `po-k serve` always runs in the foreground today.
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

    let cfg_path = config::default_config_path();
    let cfg = config::load_from(&cfg_path)
        .with_context(|| format!("loading {} (did you run `po-k init`?)", cfg_path.display()))?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    let token = Token::from_file(&token_path)
        .with_context(|| format!("loading bearer token from {}", token_path.display()))?;

    let hook_bind = cfg.hooks.bind.clone();
    let xpok = cfg.xpok.clone();

    let db_path = config::expand_path("~/.config/po-k/events.db");
    let db = events_store::open(&db_path)
        .await
        .with_context(|| format!("opening events db at {}", db_path.display()))?;
    tracing::info!(path = %db_path.display(), "events.db ready");

    let state = AppState::new(token, cfg, cfg_path.clone(), db);

    // Rebuild the Registry from the DB before accepting commands.
    if let Err(e) = crate::recovery::recover_sessions(&state).await {
        tracing::warn!(error = %e, "session recovery failed; starting clean");
    }

    // Config hot-reload (also forwards ConfigUpdate to Xpo-k via the uplink).
    config_watch::spawn(cfg_path.clone(), state.config.clone());

    // The only HTTP server po-k keeps: the localhost hook/permission listener.
    {
        let state = state.clone();
        let bind = hook_bind.clone();
        tokio::spawn(async move {
            if let Err(e) = hook_listener::serve(state, &bind).await {
                tracing::error!(error = %e, "hook listener exited");
            }
        });
    }

    // Connect to Xpo-k (the only orchestrator interface). Without it, po-k can
    // still receive CC hooks but can't be driven.
    match xpok {
        Some(x) => {
            tracing::info!(url = %x.url, "connecting to xpo-k");
            crate::xpok_client::spawn(state.clone(), x);
        }
        None => {
            tracing::warn!("no `xpok` configured — po-k will not be reachable by an orchestrator");
        }
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        hook_bind = %hook_bind,
        config = %cfg_path.display(),
        "po-k serve started"
    );
    shutdown_signal().await;
    tracing::info!("shutting down");
    Ok(())
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
