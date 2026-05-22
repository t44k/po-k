//! `po-k mcp` — stdio JSON-RPC MCP server for Claude Code. M10.5 fills this in.

use anyhow::Result;
use clap::Args as ClapArgs;

/// Stdio MCP server. Designed to be launched by Claude Code via
/// `claude mcp add po-k -- po-k mcp`. Exposes memory + skills as tools.
#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k mcp — not yet implemented (M10.5)");
    Ok(())
}
