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
        None => {
            println!("po-k gateway — JSONL stdio bridge not yet implemented (M10.7)");
            println!("(use `po-k gateway projects` to verify project discovery in the meantime)");
        }
        Some(Sub::Projects) => list_projects().await?,
    }
    Ok(())
}

async fn list_projects() -> Result<()> {
    let cfg = crate::config::load_effective()?;
    let projects = crate::project_discovery::discover(&cfg)?;
    if projects.is_empty() {
        println!("(no matching projects)");
        println!();
        println!("Checks:");
        println!("  - is `claude` actually running? (process name = `claude`)");
        println!("  - does its cwd match any `gateway.projects[].cwd` in po-k.yaml?");
        println!("  - run `po-k config` to see the merged allowlist.");
        return Ok(());
    }
    println!(
        "{:<14}{:<8}{:<10}{}",
        "slug", "cc_pid", "live", "cwd"
    );
    for p in &projects {
        println!(
            "{:<14}{:<8}{:<10}{}",
            p.slug,
            p.cc_pid,
            if p.live { "yes" } else { "no" },
            p.cwd.display()
        );
    }
    // Bonus: surface the zellij sessions we'd target if we needed to. Helpful for
    // sanity-checking the operator's setup.
    if let Ok(sessions) = crate::zellij::list_sessions() {
        if !sessions.is_empty() {
            println!();
            println!("zellij sessions: {}", sessions.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(", "));
        }
    }
    Ok(())
}
