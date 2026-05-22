//! `po-k skill` — read-side CLI on the skills folder. M10.8 fills this in.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};

/// Read or sync the cloned skills folder (`~/.cache/po-k/repo/skills/`).
#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    pub sub: Sub,
}

#[derive(Debug, Subcommand)]
pub enum Sub {
    List,
    Show { id: String },
    Sync,
}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k skill — not yet implemented (M10.8)");
    Ok(())
}
