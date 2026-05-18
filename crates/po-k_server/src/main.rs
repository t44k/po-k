use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod admin;
mod auth;
mod bootstrap;
mod bus;
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

/// po-k_server — accepts NDJSON event batches from collectors and serves the UI + MCP.
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
    /// Create a user (and auto-mint their first API key, printed once).
    UserAdd {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long, default_value = "default")]
        team: String,
        #[arg(long)]
        slug: String,
        #[arg(long, value_parser = ["admin", "member"], default_value = "member")]
        role: String,
        #[arg(long, default_value = "")]
        label: String,
    },
    /// List all users (optionally scoped to one team).
    UserList {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
        team: Option<String>,
    },
    /// Mint an additional API key for an existing user.
    Keygen {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long, default_value = "default")]
        team: String,
        #[arg(long)]
        user: String,
        #[arg(long, default_value = "")]
        label: String,
    },
    /// List all stored API keys (hash prefix, user, label, created_at). No plaintext.
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
    /// Run the distillation loop now. With no --id, processes every topic.
    Distill {
        #[arg(long, env = "PO_K_DB", default_value = "po-k.db")]
        db: PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long, env = "PO_K_LLM_BACKEND", default_value = "claude-cli")]
        backend: String,
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
        #[arg(long)]
        id: String,
        #[arg(long)]
        question: String,
        /// One of: global, global-project, user, user-project.
        #[arg(
            long,
            value_parser = ["global", "global-project", "user", "user-project"],
            default_value = "global"
        )]
        scope_kind: String,
        #[arg(long, default_value = "default")]
        team: String,
        /// User slug (required when scope_kind starts with `user`).
        #[arg(long)]
        user: Option<String>,
        /// Project slug (required when scope_kind ends with `project`).
        #[arg(long)]
        project: Option<String>,
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
            AdminCmd::UserAdd { db, team, slug, role, label } => {
                bootstrap::admin_user_add(db, team, slug, role, label).await
            }
            AdminCmd::UserList { db, team } => bootstrap::admin_user_list(db, team).await,
            AdminCmd::Keygen { db, team, user, label } => {
                bootstrap::admin_keygen(db, team, user, label).await
            }
            AdminCmd::ListKeys { db, team } => bootstrap::admin_list_keys(db, team).await,
            AdminCmd::Revoke { db, label } => bootstrap::admin_revoke(db, label).await,
            AdminCmd::Topic { cmd } => match cmd {
                TopicCmd::Add {
                    db,
                    id,
                    question,
                    scope_kind,
                    team,
                    user,
                    project,
                    extras,
                } => topics::admin_add(db, id, question, scope_kind, team, user, project, extras).await,
                TopicCmd::List { db, team } => topics::admin_list(db, team).await,
                TopicCmd::Remove { db, id } => topics::admin_remove(db, id).await,
            },
            AdminCmd::Distill { db, id, backend, model } => {
                distill::run_admin(db, id, backend, model).await
            }
        },
    }
}

async fn run_server(db: PathBuf, listen: String) -> Result<()> {
    let mut state = AppState::open(&db).await.context("opening database")?;
    state.migrate().await.context("running migrations")?;
    bootstrap::ensure_bootstrap(state.pool())
        .await
        .context("bootstrap")?;

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
            "/ui/session/:session_key/older",
            axum::routing::get(ui::transcript_older),
        )
        .route(
            "/ui/session/:session_key/ws",
            axum::routing::get(ui::transcript_ws),
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
