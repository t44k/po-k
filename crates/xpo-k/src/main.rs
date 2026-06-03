//! `xpo-k` — central profile authority and cross-container router. The sole
//! HTTP entry point for orchestrators; talks to po-k instances only over
//! WebSocket. See `~/.claude/plans/` for the full design (M14 Phase 2).

use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod cmd;
mod config;
mod http;
mod live;
mod merge;
mod registry;
mod routed;
mod state;
mod store;
mod ws;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// First-run setup: write skeleton config + generate bearer token.
    Init,
    /// Run the HTTP + WebSocket server.
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init => cmd::init().await,
        Cmd::Serve => cmd::serve().await,
    }
}
