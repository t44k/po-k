//! `po-k mcp` — stdio JSON-RPC MCP server for Claude Code.
//!
//! Designed to be launched via `claude mcp add po-k -- po-k mcp`. Reads JSON-RPC
//! requests from stdin (one per line) and writes responses to stdout.

use anyhow::Result;
use clap::Args as ClapArgs;

use crate::mcp_server;

#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(_args: Args) -> Result<()> {
    mcp_server::run().await
}
