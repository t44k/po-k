//! `po-k export-profile` — one-off migration of v1 Xpo-k profiles into CC
//! plugin directories.
//!
//!   po-k export-profile --db ~/.config/xpo-k/profiles.db --out /zirzen/base/plugins [--name n]... [--keep-env]
//!   po-k export-profile --json profile.json --out ./plugins
//!
//! Each profile becomes `<out>/<name>/` with `.claude-plugin/plugin.json`,
//! `agents/`, `skills/`, `CLAUDE.md`, `.mcp.json` and `hooks/hooks.json`.
//! Credentials in MCP `env`/`headers` are replaced by `${NAME}` unless
//! `--keep-env`; the names are printed so they can be exported on the box.
//! `profile-settings.json` keeps model/effort/etc. as a reminder to pass them
//! on `POST /sessions` — a plugin cannot carry settings.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::profile::{self, Profile};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Xpo-k `profiles.db` (read-only).
    #[arg(long, conflicts_with = "json")]
    pub db: Option<PathBuf>,
    /// A single profile as JSON (the Xpo-k `data` blob).
    #[arg(long)]
    pub json: Option<PathBuf>,
    /// Output root; one directory per profile is created inside.
    #[arg(long)]
    pub out: PathBuf,
    /// Only these profile names (default: all).
    #[arg(long = "name")]
    pub names: Vec<String>,
    /// Write MCP env/header values verbatim instead of `${NAME}` placeholders.
    #[arg(long)]
    pub keep_env: bool,
}

async fn load_from_db(path: &Path, names: &[String]) -> Result<Vec<Profile>> {
    let url = format!("sqlite://{}?mode=ro", path.display());
    let opts = SqliteConnectOptions::from_str(&url).with_context(|| format!("parsing {url}"))?.read_only(true);
    let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.with_context(|| format!("opening {}", path.display()))?;
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT name, data FROM profiles ORDER BY name")
        .fetch_all(&pool)
        .await
        .context("SELECT FROM profiles")?;
    let mut out = Vec::new();
    for (name, data) in rows {
        if !names.is_empty() && !names.contains(&name) {
            continue;
        }
        let v: Value = serde_json::from_str(&data).with_context(|| format!("profile {name}: invalid JSON"))?;
        let mut p = Profile::from_json(&v).map_err(|e| anyhow::anyhow!("profile {name}: {e}"))?;
        if p.name.is_empty() {
            p.name = name;
        }
        out.push(p);
    }
    Ok(out)
}

pub async fn run(args: Args) -> Result<()> {
    let profiles = match (&args.db, &args.json) {
        (Some(db), _) => load_from_db(db, &args.names).await?,
        (None, Some(json)) => {
            let text = std::fs::read_to_string(json).with_context(|| format!("reading {}", json.display()))?;
            let v: Value = serde_json::from_str(&text).context("invalid JSON")?;
            vec![Profile::from_json(&v).map_err(|e| anyhow::anyhow!("{e}"))?]
        }
        (None, None) => anyhow::bail!("pass --db <profiles.db> or --json <profile.json>"),
    };
    if profiles.is_empty() {
        anyhow::bail!("no profiles matched");
    }
    std::fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;
    for p in &profiles {
        let dir = args.out.join(&p.name);
        let env = profile::render_plugin_dir(&dir, p, !args.keep_env)?;
        println!("{} → {}", p.name, dir.display());
        if !env.is_empty() {
            eprintln!("  needs env on the box: {}", env.join(", "));
        }
        if p.settings.model.is_some() || p.settings.effort.is_some() || p.settings.permission_mode.is_some() || !p.settings.env.is_empty() {
            eprintln!("  settings kept in profile-settings.json — pass model/effort/permission_mode on POST /sessions");
        }
    }
    println!("exported {} plugin director{}. Use them as `plugins: [\"{}/<name>\"]`.", profiles.len(), if profiles.len() == 1 { "y" } else { "ies" }, args.out.display());
    Ok(())
}
