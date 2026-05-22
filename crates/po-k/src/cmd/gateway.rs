//! `po-k gateway` — stdio JSONL bridge for remote agents. M10.6 + M10.7 fill this in.

use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};

/// Stdio JSONL bridge. By default reads frames from stdin / writes frames to stdout
/// (this is the form an SSH-connected remote agent uses). With a subcommand it can
/// also be used for diagnostics.
#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    pub sub: Option<Sub>,
}

#[derive(Debug, Subcommand)]
pub enum Sub {
    /// Print the resolved project list (discovery + allowlist) and exit. Useful for
    /// sanity-checking zellij integration without opening a stdio bridge.
    Projects,
}

pub async fn run(args: Args) -> Result<()> {
    match args.sub {
        None => println!("po-k gateway — not yet implemented (M10.7)"),
        Some(Sub::Projects) => println!("po-k gateway projects — not yet implemented (M10.6)"),
    }
    Ok(())
}
