//! `po-k memory` — read-side CLI on the memory folder. M10.4 fills this in.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};

/// Read or sync the cloned memory folder (`~/.cache/po-k/repo/memory/`).
#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    pub sub: Sub,
}

#[derive(Debug, Subcommand)]
pub enum Sub {
    /// List topic ids with last-updated timestamp.
    List,
    /// Print one topic's digest markdown.
    Show { id: String },
    /// Pull-then-push the memory + skills repo right now.
    Sync,
}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k memory — not yet implemented (M10.4)");
    Ok(())
}
