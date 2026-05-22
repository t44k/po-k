//! `po-k` — a single client-side binary that:
//!   - taps Claude Code hooks (`po-k hook EVENT`)
//!   - distills memory + skills into markdown in a git repo (`po-k service`)
//!   - exposes that knowledge to CC over MCP (`po-k mcp`)
//!   - exposes the local CC session to a remote agent over a JSONL stdio bridge (`po-k gateway`)
//!
//! All long-running state lives in `po-k service`; everything else is a short-lived
//! subprocess that talks to the daemon over `~/.config/po-k/service.sock`.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cmd;
mod config;
mod distill;
mod gateway_proto;
mod git;
mod ipc;
mod llm;
mod mcp_server;
mod project_discovery;
mod state;
mod text;
mod turn;
mod zellij;

/// po-k — Claude Code companion (tap, distill, serve, bridge).
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Subcommand. Omit to print help + status.
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// First-run setup: write skeleton config, clone the memory + skills repo, install hooks.
    Init(cmd::init::Args),
    /// Long-running daemon: git puller, distillation loop, IPC owner.
    Service(cmd::service::Args),
    /// Stdio JSONL bridge for remote agents driving local Claude Code via zellij.
    Gateway(cmd::gateway::Args),
    /// Stdio MCP server exposing memory + skills to Claude Code.
    Mcp(cmd::mcp::Args),
    /// Claude Code hook entry point. Reads JSON on stdin, forwards to the service.
    Hook(cmd::hook::Args),
    /// Read / search the cloned memory folder.
    Memory(cmd::memory::Args),
    /// Read / search the cloned skills folder.
    Skill(cmd::skill::Args),
    /// Manually trigger topic distillation.
    Distill(cmd::distill_cmd::Args),
    /// Print the effective merged config (main + every layered repo).
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
        Some(Cmd::Service(a)) => cmd::service::run(a).await,
        Some(Cmd::Gateway(a)) => cmd::gateway::run(a).await,
        Some(Cmd::Mcp(a)) => cmd::mcp::run(a).await,
        Some(Cmd::Hook(a)) => cmd::hook::run(a).await,
        Some(Cmd::Memory(a)) => cmd::memory::run(a).await,
        Some(Cmd::Skill(a)) => cmd::skill::run(a).await,
        Some(Cmd::Distill(a)) => cmd::distill_cmd::run(a).await,
        Some(Cmd::Config(a)) => cmd::config_cmd::run(a).await,
    }
}
