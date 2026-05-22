//! `po-k hook EVENT` — the entry point Claude Code calls via the hooks block
//! in `~/.claude/settings.json`. Reads JSON from stdin, dials the service over
//! its Unix socket, forwards as one `{type:"hook",event,payload}` frame, exits.
//!
//! Latency budget: a few tens of milliseconds. We do nothing async-heavy here;
//! the daemon owns the work.

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::time::Duration;
use tokio::io::AsyncReadExt;

use crate::{config, ipc};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The Claude Code event name (e.g. `Stop`, `UserPromptSubmit`).
    pub event: String,
}

pub async fn run(args: Args) -> Result<()> {
    let mut buf = String::new();
    tokio::io::stdin().read_to_string(&mut buf).await.ok();
    let payload: serde_json::Value =
        serde_json::from_str(buf.trim()).unwrap_or(serde_json::Value::Null);

    let cfg = config::load_main().unwrap_or_default();
    let socket = config::expand_path(&cfg.service.socket);
    let req = ipc::Request::Hook {
        event: args.event.clone(),
        payload,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ipc::request(&socket, &req),
    )
    .await
    .context("hook IPC timed out — is `po-k service` running?")?;
    match result {
        Ok(_reply) => Ok(()),
        Err(e) => {
            // Hooks must not block CC; degrade silently with a stderr note.
            eprintln!("po-k hook {}: {e}", args.event);
            Ok(())
        }
    }
}
