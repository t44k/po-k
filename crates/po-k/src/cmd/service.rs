//! `po-k service` — the long-running daemon. M10.3 fills this in.

use anyhow::Result;
use clap::Args as ClapArgs;

/// Start the po-k daemon (git puller, distillation loop, IPC socket owner).
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Run in the foreground (don't detach). Default behavior; flag kept for symmetry.
    #[arg(long)]
    pub foreground: bool,
}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k service — not yet implemented (M10.3)");
    Ok(())
}
