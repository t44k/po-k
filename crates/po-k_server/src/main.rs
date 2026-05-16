use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod ingest;
mod state;

use state::AppState;

/// po-k_server — accepts NDJSON event batches from collectors and (later) serves UI + MCP.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Start the HTTP server.
    Serve {
        /// SQLite path. Created if missing.
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        /// Listen address.
        #[arg(long, env = "PO_K_LISTEN", default_value = "127.0.0.1:8787")]
        listen: String,
    },
    /// Admin operations.
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Debug, Subcommand)]
enum AdminCmd {
    /// Generate and print a new API key.
    Keygen {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long, default_value = "default")]
        team: String,
        #[arg(long, default_value = "")]
        label: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,sqlx=warn")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve { db, listen } => run_server(db, listen).await,
        Cmd::Admin { cmd } => match cmd {
            AdminCmd::Keygen { db, team, label } => admin_keygen(db, team, label).await,
        },
    }
}

async fn run_server(db: PathBuf, listen: String) -> Result<()> {
    let state = AppState::open(&db).await.context("opening database")?;
    state.migrate().await.context("running migrations")?;

    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .route("/ingest", axum::routing::post(ingest::ingest))
        .with_state(state)
        .layer(tower_http::limit::RequestBodyLimitLayer::new(128 * 1024 * 1024))
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, "po-k_server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn admin_keygen(db: PathBuf, team: String, label: String) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let key = format!("pk_{}", uuid::Uuid::now_v7().simple());
    sqlx::query("INSERT INTO api_keys (key, team_id, label) VALUES (?, ?, ?)")
        .bind(&key)
        .bind(&team)
        .bind(&label)
        .execute(state.pool())
        .await?;
    println!("{key}");
    Ok(())
}
