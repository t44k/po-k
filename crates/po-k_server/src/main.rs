use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod admin;
mod auth;
mod distill;
mod embed;
mod ingest;
mod llm;
mod mcp;
mod search;
mod state;
mod topics;
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
    /// Topic management: define questions whose answers po-k keeps distilled.
    Topic {
        #[command(subcommand)]
        cmd: TopicCmd,
    },
    /// Run the distillation loop now. With no --id, processes every topic in turn.
    Distill {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
        id: Option<String>,
        /// LLM backend to use. One of: claude-cli, anthropic, openai.
        #[arg(long, env = "PO_K_LLM_BACKEND", default_value = "claude-cli")]
        backend: String,
        /// Override the model for the chosen backend (e.g. claude-opus-4-7).
        #[arg(long, env = "PO_K_LLM_MODEL")]
        model: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum TopicCmd {
    /// Add a new topic.
    Add {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        /// kebab-case id, e.g. "auth-pattern".
        #[arg(long)]
        id: String,
        /// The question/prompt the digest should keep answering.
        #[arg(long)]
        question: String,
        /// "team" (default) or "project:<sanitized_cwd>".
        #[arg(long, default_value = "team")]
        scope: String,
        #[arg(long, default_value = "default")]
        team: String,
        /// Optional extra system prompt text appended to the LLM's instructions.
        #[arg(long)]
        extras: Option<String>,
    },
    /// List topics in a team.
    List {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long, default_value = "default")]
        team: String,
    },
    /// Remove a topic (and its digest).
    Remove {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
        id: String,
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
            AdminCmd::Topic { cmd } => match cmd {
                TopicCmd::Add {
                    db,
                    id,
                    question,
                    scope,
                    team,
                    extras,
                } => topics::admin_add(db, id, question, scope, team, extras).await,
                TopicCmd::List { db, team } => topics::admin_list(db, team).await,
                TopicCmd::Remove { db, id } => topics::admin_remove(db, id).await,
            },
            AdminCmd::Distill {
                db,
                id,
                backend,
                model,
            } => distill::run_admin(db, id, backend, model).await,
        },
    }
}

async fn run_server(db: PathBuf, listen: String) -> Result<()> {
    let mut state = AppState::open(&db).await.context("opening database")?;
    state.migrate().await.context("running migrations")?;

    // Try to load the embedder. On failure (network, ONNX runtime mismatch, etc.)
    // we just leave it unset — BM25-only search keeps working.
    match embed::FastembedEmbedder::try_load().await {
        Ok(emb) => {
            let arc: std::sync::Arc<dyn embed::Embedder> = std::sync::Arc::new(emb);
            state = state.with_embedder(arc.clone());
            let pool = state.pool().clone();
            tokio::spawn(async move {
                embed::run_indexer(pool, arc).await;
            });
        }
        Err(e) => {
            tracing::warn!(error = %e, "embedder unavailable; search will use BM25 only");
        }
    }

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
        .route("/mcp", axum::routing::post(mcp::handle))
        .route("/ui/login", axum::routing::get(admin::login_get).post(admin::login_post))
        .route("/ui/logout", axum::routing::get(admin::logout))
        .route("/ui/admin", axum::routing::get(admin::dashboard))
        .route("/ui/admin/keys", axum::routing::get(admin::keys_get).post(admin::keys_post))
        .route("/ui/admin/keys/revoke", axum::routing::post(admin::keys_revoke))
        .route(
            "/ui/admin/topics",
            axum::routing::get(admin::topics_get).post(admin::topics_post),
        )
        .route("/ui/admin/topic/:id", axum::routing::get(admin::topic_get))
        .route("/ui/admin/topic/distill", axum::routing::post(admin::topic_distill))
        .route("/ui/admin/topic/remove", axum::routing::post(admin::topic_remove))
        .route("/ui/admin/mcp", axum::routing::get(admin::mcp_get).post(admin::mcp_keygen))
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
