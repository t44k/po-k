//! `po-k init` — write skeleton `~/.config/po-k/po-k.yaml` + generate a
//! bearer token file with mode 0600. Idempotent: leaves existing files alone.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::fs;
use std::os::unix::fs::PermissionsExt;

use crate::auth;
use crate::config;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Overwrite the config file even if it already exists.
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let cfg_path = config::default_config_path();
    if let Some(parent) = cfg_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    if cfg_path.exists() && !args.force {
        tracing::info!(path = %cfg_path.display(), "config already exists — leaving it alone (use --force to overwrite)");
    } else {
        fs::write(&cfg_path, config::skeleton_yaml())
            .with_context(|| format!("writing {}", cfg_path.display()))?;
        tracing::info!(path = %cfg_path.display(), "wrote config skeleton");
    }

    // Load whatever's on disk to derive the token-file path.
    let cfg = config::load_from(&cfg_path)?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    if let Some(parent) = token_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    if token_path.exists() {
        tracing::info!(path = %token_path.display(), "auth token already exists — leaving it alone");
    } else {
        let token = auth::generate_hex_token();
        fs::write(&token_path, &token)
            .with_context(|| format!("writing {}", token_path.display()))?;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&token_path, perms)
            .with_context(|| format!("chmod 0600 {}", token_path.display()))?;
        tracing::info!(path = %token_path.display(), "generated 32-byte hex bearer token (chmod 0600)");
    }

    println!("po-k init complete.");
    println!("  config: {}", cfg_path.display());
    println!("  token:  {}", token_path.display());
    println!("Next: edit `projects:` in the config, then run `po-k serve`.");
    Ok(())
}
