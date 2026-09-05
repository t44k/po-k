//! Bare `po-k` — one-line status: config, token, bind, and whether a server
//! answers on the callback URL.

use anyhow::Result;
use std::time::Duration;

use crate::config;

pub async fn run() -> Result<()> {
    let cfg_path = config::default_config_path();
    let cfg = config::load_from(&cfg_path)?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    let token_state = if token_path.exists() { "ok" } else { "MISSING" };
    let config_state = if cfg_path.exists() { cfg_path.display().to_string() } else { "defaults".to_string() };

    let url = format!("{}/health", cfg.server.callback_base_url());
    let live = match reqwest::Client::builder().timeout(Duration::from_secs(1)).build() {
        Ok(c) => match c.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                let v: serde_json::Value = r.json().await.unwrap_or_default();
                format!(
                    "serving v{} · {} sessions · {} hosts · {} watches",
                    v["version"].as_str().unwrap_or("?"),
                    v["sessions"],
                    v["hosts"],
                    v["watches"]
                )
            }
            _ => "not running".to_string(),
        },
        Err(_) => "not running".to_string(),
    };
    println!(
        "po-k {} · config {} · token {} · bind {} · {}",
        env!("CARGO_PKG_VERSION"),
        config_state,
        token_state,
        cfg.server.bind,
        live
    );
    Ok(())
}
