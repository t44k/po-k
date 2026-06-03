//! `xpo-k init` and `xpo-k serve`.

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::str::FromStr;

use crate::auth::{generate_hex_token, Token};
use crate::config;
use crate::state::XState;
use crate::store;

pub async fn init() -> Result<()> {
    let cfg_path = config::default_config_path();
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !cfg_path.exists() {
        std::fs::write(&cfg_path, config::SKELETON)
            .with_context(|| format!("writing {}", cfg_path.display()))?;
        println!("wrote {}", cfg_path.display());
    } else {
        println!("{} already exists", cfg_path.display());
    }

    let cfg = config::load_from(&cfg_path)?;
    let token_path = config::expand_path(&cfg.auth.bearer_token_file);
    if !token_path.exists() {
        if let Some(parent) = token_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&token_path, generate_hex_token())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600));
        }
        println!("generated bearer token at {}", token_path.display());
    }
    Ok(())
}

pub async fn serve() -> Result<()> {
    let cfg_path = config::default_config_path();
    let cfg = config::load_from(&cfg_path)
        .with_context(|| format!("loading {} (did you run `xpo-k init`?)", cfg_path.display()))?;
    let token = Token::from_file(&config::expand_path(&cfg.auth.bearer_token_file))?;
    let bind = cfg.server.bind.clone();
    let addr = SocketAddr::from_str(&bind).with_context(|| format!("parsing bind {bind:?}"))?;

    let db_path = config::expand_path("~/.config/xpo-k/profiles.db");
    let db = store::open(&db_path).await?;
    tracing::info!(path = %db_path.display(), "profiles.db ready");

    let state = XState::new(cfg, token, db);
    let app = crate::http::router(state.clone()).merge(crate::ws::router(state));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, version = env!("CARGO_PKG_VERSION"), "xpo-k serve listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("axum serve")?;
    Ok(())
}
