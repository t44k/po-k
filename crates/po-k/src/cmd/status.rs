//! Bare `po-k` — prints help context + a one-screen status block.
//!
//! Right now the status is sparse because the service / config / repo aren't wired
//! up yet (M10.2+). The shape is locked: `service · repo · topics · skills`.

use anyhow::Result;

pub async fn run() -> Result<()> {
    println!("po-k {} — Claude Code companion", env!("CARGO_PKG_VERSION"));
    println!();
    println!("status");
    println!("  service: not-yet-implemented (M10.3)");
    println!("  repo:    not-yet-cloned     (M10.2)");
    println!("  topics:  0                  (M10.4)");
    println!("  skills:  0                  (M10.8)");
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
