//! `po-k init` — write the two-key `po-k.yaml` and a bearer token file
//! (mode 0600). Idempotent: existing files are left alone unless `--force`.
//! `--token <hex>` installs a fleet-wide key instead of generating one.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;

use crate::auth;
use crate::config;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Overwrite the config file even if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Use this token (e.g. the fleet key) instead of generating one. Replaces an existing token file.
    #[arg(long, env = "POK_TOKEN", hide_env_values = true)]
    pub token: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    let cfg_path = config::default_config_path();
    if let Some(parent) = cfg_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    if cfg_path.exists() && !args.force {
        tracing::info!(path = %cfg_path.display(), "config already exists — leaving it alone (use --force to overwrite)");
    } else {
        fs::write(&cfg_path, config::skeleton_yaml()).with_context(|| format!("writing {}", cfg_path.display()))?;
        tracing::info!(path = %cfg_path.display(), "wrote config");
    }

    let cfg = config::load_from(&cfg_path)?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    match args.token.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        Some(t) => {
            auth::write_token_file(&token_path, t)?;
            tracing::info!(path = %token_path.display(), "installed the provided token (chmod 0600)");
        }
        None if token_path.exists() => {
            tracing::info!(path = %token_path.display(), "auth token already exists — leaving it alone");
        }
        None => {
            auth::write_token_file(&token_path, &auth::generate_hex_token())?;
            tracing::info!(path = %token_path.display(), "generated 32-byte hex bearer token (chmod 0600)");
        }
    }

    println!("po-k init complete.");
    println!("  config: {}", cfg_path.display());
    println!("  token:  {}", token_path.display());
    println!("Next: `po-k serve`, then `curl http://127.0.0.1:{}/docs`.", cfg.server.socket_addr().map(|a| a.port()).unwrap_or(13658));
    Ok(())
}
