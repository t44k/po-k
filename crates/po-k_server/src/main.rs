use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod auth;
mod ingest;
mod search;
mod state;
mod transcript;
mod ui;

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
        /// Listen address. Default binds all interfaces; tighten in production.
        #[arg(long, env = "PO_K_LISTEN", default_value = "0.0.0.0:8787")]
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
    /// Generate and print a new API key. The plaintext is shown ONCE — we store only its blake3 hash.
    Keygen {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long, default_value = "default")]
        team: String,
        #[arg(long, default_value = "")]
        label: String,
    },
    /// List all stored API keys (label, team, created_at). No plaintext is recoverable.
    ListKeys {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
        team: Option<String>,
    },
    /// Revoke an API key by label.
    Revoke {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
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
            AdminCmd::ListKeys { db, team } => admin_list_keys(db, team).await,
            AdminCmd::Revoke { db, label } => admin_revoke(db, label).await,
        },
    }
}

async fn run_server(db: PathBuf, listen: String) -> Result<()> {
    let state = AppState::open(&db).await.context("opening database")?;
    state.migrate().await.context("running migrations")?;

    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .route("/", axum::routing::get(|| async { axum::response::Redirect::to("/ui") }))
        .route("/ingest", axum::routing::post(ingest::ingest))
        .route(
            "/ingest/subagent-meta",
            axum::routing::post(ingest::ingest_subagent_meta),
        )
        .route("/ui", axum::routing::get(ui::projects))
        .route("/ui/project/:sanitized_cwd", axum::routing::get(ui::sessions))
        .route("/ui/session/:session_key", axum::routing::get(ui::transcript))
        .route(
            "/ui/session/:session_key/page",
            axum::routing::get(ui::transcript_page),
        )
        .route("/ui/search", axum::routing::get(ui::search))
        .route("/api/search", axum::routing::get(ui::api_search))
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(128 * 1024 * 1024))
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
    // Make sure the team exists; create it on the fly if not (the `default` team is
    // already seeded, but operators may want named teams).
    sqlx::query("INSERT OR IGNORE INTO teams (id, label) VALUES (?, ?)")
        .bind(&team)
        .bind(&team)
        .execute(state.pool())
        .await?;
    let key = format!("pk_{}", uuid::Uuid::now_v7().simple());
    let key_hash = auth::hash_api_key(&key);
    sqlx::query("INSERT INTO api_keys (key_hash, team_id, label) VALUES (?, ?, ?)")
        .bind(&key_hash)
        .bind(&team)
        .bind(&label)
        .execute(state.pool())
        .await?;
    println!("{key}");
    eprintln!("# saved with team={team} label={label}. This is shown ONCE.");
    Ok(())
}

async fn admin_list_keys(db: PathBuf, team: Option<String>) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let rows = match team.as_deref() {
        Some(t) => sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT substr(key_hash, 1, 12), team_id, label, created_at
             FROM api_keys WHERE team_id = ? ORDER BY created_at",
        )
        .bind(t)
        .fetch_all(state.pool())
        .await?,
        None => sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT substr(key_hash, 1, 12), team_id, label, created_at
             FROM api_keys ORDER BY team_id, created_at",
        )
        .fetch_all(state.pool())
        .await?,
    };
    if rows.is_empty() {
        println!("(no keys)");
    } else {
        println!("{:<14}{:<14}{:<24}{}", "hash_prefix", "team", "label", "created_at");
        for (h, team, label, created) in rows {
            println!("{:<14}{:<14}{:<24}{}", h, team, label, created);
        }
    }
    Ok(())
}

async fn admin_revoke(db: PathBuf, label: String) -> Result<()> {
    let state = AppState::open(&db).await?;
    state.migrate().await?;
    let r = sqlx::query("DELETE FROM api_keys WHERE label = ?")
        .bind(&label)
        .execute(state.pool())
        .await?;
    println!("revoked {} key(s) with label '{label}'", r.rows_affected());
    Ok(())
}
