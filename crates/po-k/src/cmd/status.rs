//! Bare `po-k` — prints help + a one-screen status block.
//!
//! Tries the running daemon over its Unix socket; if the socket isn't reachable
//! within ~50ms we fall back to a static "service: not running" line and read the
//! repo state straight from disk.

use anyhow::Result;
use std::time::Duration;

use crate::{config, ipc};

const PROBE_TIMEOUT: Duration = Duration::from_millis(150);

pub async fn run() -> Result<()> {
    println!("po-k {} — Claude Code companion", env!("CARGO_PKG_VERSION"));
    println!();
    println!("status");

    let cfg = config::load_main().unwrap_or_default();
    let cfg_path = config::main_config_path();
    if !cfg_path.exists() {
        println!(
            "  config:  missing — run `po-k init` (would write {})",
            cfg_path.display()
        );
    } else {
        println!("  config:  {}", cfg_path.display());
    }

    // Probe the daemon.
    let socket = config::expand_path(&cfg.service.socket);
    let probe = tokio::time::timeout(PROBE_TIMEOUT, ipc::request(&socket, &ipc::Request::Status))
        .await
        .ok()
        .and_then(|r| r.ok());

    match probe {
        Some(ipc::Reply::Status {
            pid,
            started_at,
            repo,
            topic_count,
            skill_count,
        }) => {
            println!("  service: running (pid {pid}, since {started_at})");
            match repo {
                Some(r) => {
                    let last = r
                        .last_pull_at
                        .map(|t| format!("{}{}", t, if r.last_pull_ok { "" } else { " (FAILED)" }))
                        .unwrap_or_else(|| "never".to_string());
                    println!("  repo:    {} (last pull: {last})", r.path.display());
                }
                None => println!("  repo:    (no `repo:` block in config)"),
            }
            println!("  topics:  {topic_count}");
            println!("  skills:  {skill_count}");
        }
        _ => {
            // Daemon not running; fall back to a disk read.
            let s = config::status();
            println!("  service: not running (start with `po-k service --foreground`)");
            match (s.repo_path.as_ref(), s.repo_present) {
                (Some(p), true) => println!("  repo:    {} (last pull: unknown)", p.display()),
                (Some(p), false) => println!("  repo:    {} (not cloned — run `po-k init`)", p.display()),
                (None, _) => println!("  repo:    (no `repo:` block in config)"),
            }
            println!("  topics:  {}", s.topic_count);
            println!("  skills:  {}", s.skill_count);
        }
    }

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
