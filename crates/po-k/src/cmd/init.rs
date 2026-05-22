//! `po-k init` — first-run setup.
//!
//! Steps (idempotent):
//!   1. Ensure `~/.config/po-k/po-k.yaml` exists. If missing, write the skeleton
//!      and stop with a "edit it, then re-run" message — initial setup requires a
//!      repo URL the user has to fill in.
//!   2. If the config has a repo URL set and the repo isn't cloned yet, run
//!      `git clone <url> <path>`.
//!   3. Show a diff of the proposed hooks block against `~/.claude/settings.json`
//!      and apply on confirm (or `--yes`).
//!   4. Print MCP wiring instructions.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::config;

/// First-run setup: write skeleton config, clone the configured repo, install hooks.
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Skip the hooks-install confirmation prompt.
    #[arg(long)]
    pub yes: bool,
    /// Skip the hooks-install step entirely.
    #[arg(long)]
    pub no_hooks: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let cfg_path = config::main_config_path();

    // Step 1 — config file.
    if !cfg_path.exists() {
        if let Some(parent) = cfg_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&cfg_path, config::SKELETON_YAML)
            .with_context(|| format!("writing {}", cfg_path.display()))?;
        println!("wrote skeleton config: {}", cfg_path.display());
        println!();
        println!("Next:");
        println!("  1. Edit {} and set `repo.url`.", cfg_path.display());
        println!("  2. Re-run `po-k init` to clone + install hooks.");
        return Ok(());
    }

    let cfg = config::load_main()?;
    let Some(repo) = cfg.repo.as_ref().filter(|r| !r.url.is_empty()) else {
        println!("config exists at {}", cfg_path.display());
        println!("but `repo.url` isn't set yet. Edit the file, then re-run `po-k init`.");
        return Ok(());
    };

    // Step 2 — clone if needed.
    let repo_path = config::expand_path(&repo.path);
    if repo_path.join(".git").exists() {
        println!("repo already cloned: {}", repo_path.display());
    } else {
        if let Some(parent) = repo_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        println!("cloning {} → {} ...", repo.url, repo_path.display());
        let status = std::process::Command::new("git")
            .args(["clone", "--branch", &repo.branch, &repo.url])
            .arg(&repo_path)
            .status()
            .context("spawning git clone (is `git` on PATH?)")?;
        if !status.success() {
            anyhow::bail!("git clone failed (exit {status})");
        }
        // Ensure the canonical layout exists, even if the repo is empty.
        let _ = std::fs::create_dir_all(repo_path.join("memory"));
        let _ = std::fs::create_dir_all(repo_path.join("skills"));
    }

    // Step 3 — Claude Code hooks.
    if !args.no_hooks {
        install_hooks(args.yes).context("installing hooks")?;
    }

    // Step 4 — MCP wiring instructions.
    println!();
    println!("MCP setup");
    println!("  add po-k to Claude Code's stdio MCP set with:");
    println!("    claude mcp add po-k -- po-k mcp");
    println!();
    println!("Start the service in the background to begin distillation:");
    println!("    po-k service --foreground &     # or set up a user systemd unit");
    println!();
    println!("Open a JSONL gateway for a remote agent:");
    println!("    ssh you@here po-k gateway");
    Ok(())
}

// ─── hooks install ───────────────────────────────────────────────────────────

const HOOKS_BLOCK: &str = r#"{
  "UserPromptSubmit": [{"matcher": "", "hooks": [{"type": "command", "command": "po-k hook UserPromptSubmit"}]}],
  "Stop":             [{"matcher": "", "hooks": [{"type": "command", "command": "po-k hook Stop"}]}],
  "SubagentStop":     [{"matcher": "", "hooks": [{"type": "command", "command": "po-k hook SubagentStop"}]}],
  "PostToolUse":      [{"matcher": "", "hooks": [{"type": "command", "command": "po-k hook PostToolUse"}]}]
}"#;

fn settings_path() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".claude/settings.json")
}

fn install_hooks(non_interactive: bool) -> Result<()> {
    let path = settings_path();
    let existing: serde_json::Value = if path.exists() {
        serde_json::from_str(&std::fs::read_to_string(&path)?)
            .with_context(|| format!("parsing {}", path.display()))?
    } else {
        serde_json::json!({})
    };

    let desired: serde_json::Value = serde_json::from_str(HOOKS_BLOCK)?;
    let already_has = matches!(
        existing.get("hooks"),
        Some(v) if v == &desired
    );
    if already_has {
        println!("hooks already installed in {}", path.display());
        return Ok(());
    }

    let mut next = existing.clone();
    next.as_object_mut()
        .map(|m| m.insert("hooks".into(), desired.clone()));

    println!();
    println!("proposed update to {}:", path.display());
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({ "hooks": &desired }))?
    );

    if !non_interactive {
        print!("apply? [y/N] ");
        std::io::stdout().flush().ok();
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes") {
            println!("skipped.");
            return Ok(());
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let serialized = serde_json::to_string_pretty(&next)?;
    std::fs::write(&path, serialized)?;
    println!("installed hooks → {}", path.display());
    let _ = walk(&path);
    Ok(())
}

fn walk(_p: &Path) -> Result<()> {
    Ok(())
}
