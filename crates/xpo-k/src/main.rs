//! `xpo-k` — central profile authority and cross-container router. The sole
//! HTTP entry point for orchestrators; talks to po-k instances only over
//! WebSocket. See `~/.claude/plans/` for the full design (M14 Phase 2).

use anyhow::Result;
use clap::{Parser, Subcommand};

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
        Cmd::Init => xpo_k::cmd::init().await,
        Cmd::Serve => xpo_k::cmd::serve().await,
    }
}
