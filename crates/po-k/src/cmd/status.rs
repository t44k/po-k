//! Bare `po-k` — one-line status.
//!
//! For M11.1 this just prints config + token state and a hint. Once `po-k serve`
//! exposes /health it'll probe the bind address for liveness.

use anyhow::Result;

use crate::config;

pub async fn run() -> Result<()> {
    let cfg_path = config::default_config_path();
    if !cfg_path.exists() {
        println!("po-k: no config at {}.", cfg_path.display());
        println!("Run `po-k init` to generate one.");
        return Ok(());
    }

    let cfg = config::load_from(&cfg_path)?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    let token_state = if token_path.exists() { "ok" } else { "MISSING" };

    println!(
        "po-k {} · config {} · token {} · {} projects · bind {}",
        env!("CARGO_PKG_VERSION"),
        cfg_path.display(),
        token_state,
        cfg.projects.len(),
        cfg.server.bind,
    );
    Ok(())
}
