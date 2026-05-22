//! `po-k init` — first-run setup. M10.2 fills this in.

use anyhow::Result;
use clap::Args as ClapArgs;

/// First-run setup: write a skeleton config, clone the configured repo, install Claude
/// Code hooks, print MCP + zellij wiring instructions. Idempotent.
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Skip the hook-install confirmation diff (use in scripts).
    #[arg(long)]
    pub yes: bool,
}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k init — not yet implemented (M10.2)");
    Ok(())
}
