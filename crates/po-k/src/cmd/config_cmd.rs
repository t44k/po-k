//! `po-k config` — print the effective merged config. M10.2 fills this in.

use anyhow::Result;
use clap::Args as ClapArgs;

/// Dump the effective merged config (main `~/.config/po-k/po-k.yaml` plus every
/// layered per-repo override) to stdout in YAML form.
#[derive(Debug, ClapArgs)]
pub struct Args {}

pub async fn run(_args: Args) -> Result<()> {
    println!("po-k config — not yet implemented (M10.2)");
    Ok(())
}
