//! `po-k hook EVENT` — Claude Code hook entry point. M10.4 fills this in.
//!
//! Called by `~/.claude/settings.json` hook config. Reads the CC-supplied JSON from
//! stdin, dials `~/.config/po-k/service.sock`, forwards as one frame, exits. Must
//! complete in ~tens of milliseconds because CC blocks on hooks.

use anyhow::Result;
use clap::Args as ClapArgs;

/// Hook entry point. Reads JSON from stdin and forwards to the service.
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The Claude Code event name (e.g. `Stop`, `UserPromptSubmit`).
    pub event: String,
}

pub async fn run(args: Args) -> Result<()> {
    println!("po-k hook {} — not yet implemented (M10.4)", args.event);
    Ok(())
}
