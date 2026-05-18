//! Server-rendered admin UI: dashboard, API keys, topics + digests, MCP setup.
//!
//! Auth model: the user logs in with an existing API key on /ui/login; the key is stored
//! in an HttpOnly `po-k-key` cookie and looked up per request via `auth::lookup`. Every
//! handler that does anything write-side calls `require_admin` first. Public pages
//! (/ui, /ui/project/*, /ui/session/*, /ui/search) stay open.
//!
//! No CSRF protection in v1 — admin is intended for trusted networks. Run behind a VPN
//! or reverse-proxy auth if you expose this to the open internet.

use anyhow::Result;
use askama::Template;
use axum::{
    extract::{Path, State},
    http::{header::SET_COOKIE, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use serde::Deserialize;
use sqlx::Row;
use std::sync::Arc;

use crate::auth::{self, AuthCtx};
use crate::distill;
use crate::llm;
use crate::state::AppState;
use crate::topics::{self, TopicWithDigest};

const COOKIE_NAME: &str = "po-k-key";

// ─── auth helpers ─────────────────────────────────────────────────────────────

fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for piece in raw.split(';') {
        let piece = piece.trim();
        if let Some((k, v)) = piece.split_once('=') {
            if k == name {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn current_admin(state: &AppState, headers: &HeaderMap) -> Option<AuthCtx> {
    let key = read_cookie(headers, COOKIE_NAME)?;
    auth::lookup(state.pool(), &key).await.ok().flatten()
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<AuthCtx, Response> {
    match current_admin(state, headers).await {
        Some(ctx) if ctx.role.is_admin() => Ok(ctx),
        Some(_) => Err((StatusCode::FORBIDDEN, "admin role required").into_response()),
        None => Err(Redirect::to("/ui/login").into_response()),
    }
}

// ─── login / logout ───────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTpl {
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    api_key: String,
}

pub async fn login_get() -> Response {
    render(LoginTpl { error: None })
}

pub async fn login_post(
    State(state): State<AppState>,
    Form(form): Form<LoginForm>,
) -> Response {
    match auth::lookup(state.pool(), form.api_key.trim()).await {
        Ok(Some(_)) => {
            let cookie = format!(
                "{COOKIE_NAME}={}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000",
                form.api_key.trim()
            );
            let mut resp = Redirect::to("/ui/admin").into_response();
            if let Ok(v) = cookie.parse() {
                resp.headers_mut().insert(SET_COOKIE, v);
            }
            resp
        }
        Ok(None) => render(LoginTpl {
            error: Some("That key isn't valid. Mint one with `po-k_server admin keygen` or via the admin UI on a logged-in browser.".into()),
        }),
        Err(e) => render(LoginTpl {
            error: Some(format!("auth lookup failed: {e}")),
        }),
    }
}

pub async fn logout() -> Response {
    let clear = format!("{COOKIE_NAME}=; Path=/; HttpOnly; Max-Age=0");
    let mut resp = Redirect::to("/ui/login").into_response();
    if let Ok(v) = clear.parse() {
        resp.headers_mut().insert(SET_COOKIE, v);
    }
    resp
}

// ─── dashboard ────────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "admin/dashboard.html")]
struct DashboardTpl {
    team: String,
    sessions: i64,
    events: i64,
    embedded: i64,
    embedded_pct: i64,
    machines: i64,
    keys: i64,
    topics: i64,
    digests: i64,
    last_activity: Option<String>,
}

pub async fn dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let team = admin.team_id;
    let pool = state.pool();

    async fn count_team(pool: &sqlx::SqlitePool, sql: &str, team: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(sql)
            .bind(team)
            .fetch_one(pool)
            .await
            .unwrap_or(0)
    }

    let sessions = count_team(pool, "SELECT COUNT(*) FROM sessions WHERE team_id = ?", &team).await;
    let events = count_team(pool, "SELECT COUNT(*) FROM events WHERE team_id = ?", &team).await;
    let embedded = count_team(pool, "SELECT COUNT(*) FROM events_embedding WHERE team_id = ?", &team).await;
    let machines = count_team(pool, "SELECT COUNT(*) FROM machines WHERE team_id = ?", &team).await;
    let keys = count_team(
        pool,
        "SELECT COUNT(*) FROM api_keys k JOIN users u ON u.id = k.user_id WHERE u.team_id = ?",
        &team,
    )
    .await;
    let topics = count_team(pool, "SELECT COUNT(*) FROM topics WHERE team_id = ?", &team).await;
    let digests = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM digests d JOIN topics t ON t.id = d.topic_id WHERE t.team_id = ?",
    )
    .bind(&team)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let last_activity: Option<String> = sqlx::query_scalar(
        "SELECT MAX(last_event_at) FROM sessions WHERE team_id = ?",
    )
    .bind(&team)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    let embedded_pct = if events > 0 {
        (embedded * 100) / events
    } else {
        0
    };

    render(DashboardTpl {
        team,
        sessions,
        events,
        embedded,
        embedded_pct,
        machines,
        keys,
        topics,
        digests,
        last_activity,
    })
}

// ─── keys ─────────────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "admin/keys.html")]
struct KeysTpl {
    team: String,
    rows: Vec<KeyRow>,
    minted: Option<MintedKey>,
}

struct KeyRow {
    hash_prefix: String,
    label: String,
    created_at: String,
    user_slug: String,
}

struct MintedKey {
    plaintext: String,
    label: String,
}

#[derive(Deserialize)]
pub struct KeygenForm {
    label: String,
}

pub async fn keys_get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    render_keys(&state, &admin.team_id, None).await
}

pub async fn keys_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<KeygenForm>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let label = form.label.trim().to_string();
    if label.is_empty() {
        return render_keys(&state, &admin.team_id, None).await;
    }
    let (plaintext, _hash) = match crate::bootstrap::mint_key_for(&admin.user_id, state.pool(), &label).await {
        Ok(p) => p,
        Err(e) => return server_error(format!("insert key: {e}")),
    };
    render_keys(
        &state,
        &admin.team_id,
        Some(MintedKey {
            plaintext,
            label,
        }),
    )
    .await
}

#[derive(Deserialize)]
pub struct RevokeForm {
    label: String,
}

pub async fn keys_revoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<RevokeForm>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    if let Err(e) = sqlx::query(
        "DELETE FROM api_keys
         WHERE label = ? AND user_id IN (SELECT id FROM users WHERE team_id = ?)",
    )
    .bind(&form.label)
    .bind(&admin.team_id)
    .execute(state.pool())
    .await
    {
        return server_error(format!("delete key: {e}"));
    }
    Redirect::to("/ui/admin/keys").into_response()
}

async fn render_keys(state: &AppState, team: &str, minted: Option<MintedKey>) -> Response {
    let rows = match sqlx::query(
        "SELECT substr(k.key_hash, 1, 12) AS hash_prefix, k.label, k.created_at, u.slug AS user_slug
         FROM api_keys k JOIN users u ON u.id = k.user_id
         WHERE u.team_id = ?
         ORDER BY k.created_at DESC",
    )
    .bind(team)
    .fetch_all(state.pool())
    .await
    {
        Ok(r) => r
            .into_iter()
            .map(|r| KeyRow {
                hash_prefix: r.try_get("hash_prefix").unwrap_or_default(),
                label: r.try_get("label").unwrap_or_default(),
                created_at: r.try_get("created_at").unwrap_or_default(),
                user_slug: r.try_get("user_slug").unwrap_or_default(),
            })
            .collect(),
        Err(e) => return server_error(format!("list keys: {e}")),
    };
    render(KeysTpl {
        team: team.to_string(),
        rows,
        minted,
    })
}

// ─── topics ───────────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "admin/topics.html")]
struct TopicsTpl {
    team: String,
    rows: Vec<TopicWithDigest>,
}

#[derive(Template)]
#[template(path = "admin/topic.html")]
struct TopicTpl {
    team: String,
    topic: TopicWithDigest,
}

#[derive(Deserialize)]
pub struct TopicForm {
    id: String,
    question: String,
    #[serde(default)]
    extras: String,
}

pub async fn topics_get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let rows = match topics::list_with_digests(state.pool(), &admin.team_id).await {
        Ok(r) => r,
        Err(e) => return server_error(format!("list topics: {e}")),
    };
    render(TopicsTpl {
        team: admin.team_id,
        rows,
    })
}

pub async fn topics_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<TopicForm>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let id = form.id.trim().to_string();
    let question = form.question.trim().to_string();
    if id.is_empty() || question.is_empty() {
        return Redirect::to("/ui/admin/topics").into_response();
    }
    let extras = if form.extras.trim().is_empty() {
        None
    } else {
        Some(form.extras.trim().to_string())
    };
    // M9.1: topics created through the UI are global-only. The scope_kind picker
    // (with user / project selectors) lands in M9.5.
    if let Err(e) = topics::add(
        state.pool(),
        &topics::Topic {
            id,
            team_id: admin.team_id,
            scope_kind: "global".to_string(),
            user_id: None,
            project_id: None,
            question,
            system_prompt_extras: extras,
        },
    )
    .await
    {
        return server_error(format!("add topic: {e}"));
    }
    Redirect::to("/ui/admin/topics").into_response()
}

pub async fn topic_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let topic = match topics::get_with_digest(state.pool(), &id).await {
        Ok(Some(t)) if t.topic.team_id == admin.team_id => t,
        Ok(_) => return (StatusCode::NOT_FOUND, "topic not found").into_response(),
        Err(e) => return server_error(format!("load topic: {e}")),
    };
    render(TopicTpl {
        team: admin.team_id,
        topic,
    })
}

#[derive(Deserialize)]
pub struct TopicDelete {
    id: String,
}

pub async fn topic_remove(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<TopicDelete>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    if let Err(e) = sqlx::query(
        "DELETE FROM topics WHERE id = ? AND team_id = ?",
    )
    .bind(&form.id)
    .bind(&admin.team_id)
    .execute(state.pool())
    .await
    {
        return server_error(format!("delete topic: {e}"));
    }
    Redirect::to("/ui/admin/topics").into_response()
}

pub async fn topic_distill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<TopicDelete>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    // Confirm the topic belongs to this team before scheduling work.
    match topics::get(state.pool(), &form.id).await {
        Ok(Some(t)) if t.team_id == admin.team_id => {}
        Ok(_) => return (StatusCode::NOT_FOUND, "topic not found").into_response(),
        Err(e) => return server_error(format!("load topic: {e}")),
    }

    // Distillation takes ~tens of seconds; do it on a background task so the form
    // submit returns immediately. The version column will tick up when it finishes.
    let pool = state.pool().clone();
    let id = form.id.clone();
    tokio::spawn(async move {
        let llm = match llm::from_config("claude-cli", None) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "llm config failed");
                return;
            }
        };
        if let Err(e) = distill::distill_one(&pool, &id, llm.as_ref()).await {
            tracing::error!(topic = %id, error = %e, "distill task failed");
        }
    });

    Redirect::to("/ui/admin/topics").into_response()
}

// ─── MCP setup ────────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "admin/mcp.html")]
struct McpSetupTpl {
    team: String,
    server_url: String,
    minted: Option<MintedKey>,
}

#[derive(Deserialize)]
pub struct McpKeygenForm {
    label: String,
}

pub async fn mcp_get(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let server_url = detect_server_url(&headers);
    render(McpSetupTpl {
        team: admin.team_id,
        server_url,
        minted: None,
    })
}

pub async fn mcp_keygen(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<McpKeygenForm>,
) -> Response {
    let admin = match require_admin(&state, &headers).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    let label = form.label.trim().to_string();
    if label.is_empty() {
        return Redirect::to("/ui/admin/mcp").into_response();
    }
    let (plaintext, _hash) = match crate::bootstrap::mint_key_for(&admin.user_id, state.pool(), &label).await {
        Ok(p) => p,
        Err(e) => return server_error(format!("insert key: {e}")),
    };
    let server_url = detect_server_url(&headers);
    render(McpSetupTpl {
        team: admin.team_id,
        server_url,
        minted: Some(MintedKey {
            plaintext,
            label,
        }),
    })
}

fn detect_server_url(headers: &HeaderMap) -> String {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:8787");
    // We don't know the scheme reliably behind a proxy; default to http for local dev.
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    format!("{scheme}://{host}")
}

// ─── current admin (for templates) ────────────────────────────────────────────

/// Expose the current admin's team to the layout (so we can show "logged in as …").
/// Returns "" when not logged in — the template renders a Log in link instead.
pub async fn current_team(state: &AppState, headers: &HeaderMap) -> String {
    current_admin(state, headers)
        .await
        .map(|a| a.team_id)
        .unwrap_or_default()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

fn render<T: Template>(t: T) -> Response {
    match t.render() {
        Ok(s) => Html(s).into_response(),
        Err(e) => server_error(format!("template: {e}")),
    }
}

fn server_error(msg: impl std::fmt::Display) -> Response {
    tracing::error!(error = %msg, "admin error");
    (StatusCode::INTERNAL_SERVER_ERROR, format!("error: {msg}")).into_response()
}
