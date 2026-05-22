//! `po-k distill` — manual distillation trigger. M10.4 fills this in.

use anyhow::Result;
use clap::Args as ClapArgs;

/// Run the distillation loop now. With no --topic, runs every topic in turn.
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Topic id. If omitted, all topics.
    #[arg(long)]
    pub topic: Option<String>,
}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k distill — not yet implemented (M10.4)");
    Ok(())
}
