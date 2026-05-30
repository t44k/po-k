//! `po-k` — HTTP service for driving Claude Code instances inside dedicated
//! zellij sessions. See `~/.claude/plans/this-is-a-brand-shimmying-cocoa.md`
//! for the full architecture (M11).
//!
//! Subcommands:
//!   - `po-k init`   — write skeleton config + generate bearer token
//!   - `po-k serve`  — the HTTP server (axum) that owns everything
//!   - `po-k mcp`    — stdio MCP server (launched by CC; M11.8)
//!   - `po-k config` — print effective config
//!   - bare `po-k`   — status line

use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod cmd;
mod config;
mod config_watch;
mod event_bus;
mod events_store;
mod http;
mod jsonl_tail;
mod permissions;
mod recovery;
mod session;
mod state;
mod status;
mod systemd_install;
mod zellij;

/// po-k — drive Claude Code over zellij via a small HTTP service.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// First-run setup: write skeleton config + generate bearer token.
    Init(cmd::init::Args),
    /// Run the HTTP service.
    Serve(cmd::serve::Args),
    /// Stdio MCP server (launched by CC via the per-session mcp.json).
    Mcp(cmd::mcp::Args),
    /// Print the effective merged config.
    Config(cmd::config_cmd::Args),
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
        None => cmd::status::run().await,
        Some(Cmd::Init(a)) => cmd::init::run(a).await,
        Some(Cmd::Serve(a)) => cmd::serve::run(a).await,
        Some(Cmd::Mcp(a)) => cmd::mcp::run(a).await,
        Some(Cmd::Config(a)) => cmd::config_cmd::run(a).await,
    }
}
