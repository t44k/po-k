//! Bare `po-k` — prints help context + a one-screen status block.
//!
//! Reads `~/.config/po-k/po-k.yaml` (treats missing as "not yet init'd") and reports:
//!   service state (M10.3), repo presence + last-pull (M10.2 partial), topic + skill
//!   counts derived from markdown files in the cloned repo.

use anyhow::Result;

use crate::config;

pub async fn run() -> Result<()> {
    let s = config::status();

    println!("po-k {} — Claude Code companion", env!("CARGO_PKG_VERSION"));
    println!();
    println!("status");
    if !s.config_exists {
        println!(
            "  config:  missing — run `po-k init` (would write {})",
            s.config_path.display()
        );
    } else {
        println!("  config:  {}", s.config_path.display());
    }
    println!("  service: not-yet-implemented (M10.3)");
    match (s.repo_path.as_ref(), s.repo_present) {
        (Some(p), true) => {
            let last = s.last_pull.as_deref().unwrap_or("unknown");
            println!("  repo:    {} (last pull: {last})", p.display());
        }
        (Some(p), false) => {
            println!("  repo:    {} (not cloned — run `po-k init`)", p.display());
        }
        (None, _) => {
            println!("  repo:    (no `repo:` block in config)");
        }
    }
    println!("  topics:  {}", s.topic_count);
    println!("  skills:  {}", s.skill_count);
    println!();
    println!("subcommands  (`po-k <subcommand> --help` for details)");
    println!("  init      first-run setup: config, repo clone, hook install");
    println!("  service   long-running daemon (puller + distiller + IPC owner)");
    println!("  gateway   stdio JSONL bridge for remote agents");
    println!("  mcp       stdio MCP server exposing memory + skills to Claude Code");
    println!("  hook      Claude Code hook entry point (called by CC)");
    println!("  memory    read / search the memory folder");
    println!("  skill     read / search the skills folder");
    println!("  distill   manually run topic distillation");
    println!("  config    print the effective merged config");
    Ok(())
}
