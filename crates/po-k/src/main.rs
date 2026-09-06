//! `po-k` — drive Claude Code sessions inside zellij over HTTP, with a hub and
//! an MCP front-end for agents.
//!
//! Subcommands:
//!   - `po-k serve`           — the HTTP API + hub (runs on every box)
//!   - `po-k mcp`             — stdio MCP server for an agent; talks to the local serve
//!   - `po-k cc-mcp`          — the per-session permission shim Claude Code launches
//!   - `po-k init`            — write config + token file
//!   - `po-k config`          — print the effective config
//!   - `po-k export-profile`  — v1 Xpo-k profiles → CC plugin directories
//!   - bare `po-k`            — status line

use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod cc_trust;
mod cmd;
mod config;
mod core;
mod defaults;
mod event_bus;
mod events_store;
mod http;
mod hub;
mod jsonl_tail;
mod mcp_stdio;
mod permissions;
mod profile;
mod recovery;
mod session;
mod state;
mod status;
mod systemd_install;
mod version;
mod zellij;

/// po-k — drive Claude Code over zellij via HTTP; hub + MCP for agents.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// First-run setup: write config + bearer token file.
    Init(cmd::init::Args),
    /// Run the HTTP API + hub.
    Serve(cmd::serve::Args),
    /// Stdio MCP server for an agent (Hermes); requires a local `po-k serve`.
    Mcp(cmd::mcp::Args),
    /// Stdio MCP permission shim launched by Claude Code (internal).
    #[command(name = "cc-mcp")]
    CcMcp(cmd::cc_mcp::Args),
    /// Print the effective config.
    Config(cmd::config_cmd::Args),
    /// Convert v1 Xpo-k profiles into CC plugin directories.
    #[command(name = "export-profile")]
    ExportProfile(cmd::export_profile::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        None => cmd::status::run().await,
        Some(Cmd::Init(a)) => cmd::init::run(a).await,
        Some(Cmd::Serve(a)) => cmd::serve::run(a).await,
        Some(Cmd::Mcp(a)) => cmd::mcp::run(a).await,
        Some(Cmd::CcMcp(a)) => cmd::cc_mcp::run(a).await,
        Some(Cmd::Config(a)) => cmd::config_cmd::run(a).await,
        Some(Cmd::ExportProfile(a)) => cmd::export_profile::run(a).await,
    }
}
