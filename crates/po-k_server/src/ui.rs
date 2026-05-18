//! Server-rendered UI: project list, session list, transcript view.

use askama::Template;
use axum::{
    extract::{ws::Message, ws::WebSocket, ws::WebSocketUpgrade, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json, Response},
};
use serde::Deserialize;
use sqlx::Row;

use crate::auth;
use crate::search::{self, Hit};
use crate::state::AppState;
use crate::transcript::build_turns_html;
use po_k_proto::HEADER_API_KEY;

const PAGE_SIZE: i64 = 200;
const OLDER_PAGE_SIZE: i64 = 100;

// ─── Project list ─────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "projects.html")]
struct ProjectsTpl {
    rows: Vec<ProjectRow>,
}

struct ProjectRow {
    sanitized_cwd: String,
    session_count: i64,
    event_count: i64,
    last_event_at: Option<String>,
}

pub async fn projects(State(state): State<AppState>) -> Response {
    let result = sqlx::query(
        "SELECT sanitized_cwd,
                COUNT(*) AS session_count,
                COALESCE(SUM(event_count), 0) AS event_count,
                MAX(last_event_at) AS last_event_at
         FROM sessions
         GROUP BY sanitized_cwd
         ORDER BY MAX(last_event_at) DESC",
    )
    .fetch_all(state.pool())
    .await;

    let rows = match result {
        Ok(r) => r
            .into_iter()
            .map(|row| ProjectRow {
                sanitized_cwd: row.try_get("sanitized_cwd").unwrap_or_default(),
                session_count: row.try_get("session_count").unwrap_or(0),
                event_count: row.try_get("event_count").unwrap_or(0),
                last_event_at: row.try_get("last_event_at").ok(),
            })
            .collect(),
        Err(e) => return server_error(e),
    };

    render(ProjectsTpl { rows })
}

// ─── Session list ─────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "sessions.html")]
struct SessionsTpl {
    sanitized_cwd: String,
    rows: Vec<SessionRow>,
}

struct SessionRow {
    session_key: String,
    session_uuid: String,
    title: Option<String>,
    event_count: i64,
    first_event_at: Option<String>,
    last_event_at: Option<String>,
}

pub async fn sessions(
    State(state): State<AppState>,
    Path(sanitized_cwd): Path<String>,
) -> Response {
    let result = sqlx::query(
        "SELECT s.session_key, s.session_uuid, s.event_count, s.first_event_at, s.last_event_at,
                (SELECT json_extract(CAST(raw AS TEXT), '$.aiTitle')
                   FROM events e
                  WHERE e.session_key = s.session_key AND e.kind = 'ai-title'
                  ORDER BY e.line_no DESC LIMIT 1) AS title
         FROM sessions s
         WHERE s.sanitized_cwd = ?
         ORDER BY s.last_event_at DESC",
    )
    .bind(&sanitized_cwd)
    .fetch_all(state.pool())
    .await;

    let rows = match result {
        Ok(r) => r
            .into_iter()
            .map(|row| SessionRow {
                session_key: row.try_get("session_key").unwrap_or_default(),
                session_uuid: row.try_get("session_uuid").unwrap_or_default(),
                title: row.try_get("title").ok(),
                event_count: row.try_get("event_count").unwrap_or(0),
                first_event_at: row.try_get("first_event_at").ok(),
                last_event_at: row.try_get("last_event_at").ok(),
            })
            .collect(),
        Err(e) => return server_error(e),
    };

    render(SessionsTpl { sanitized_cwd, rows })
}

// ─── Transcript ───────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "transcript.html")]
struct TranscriptTpl {
    session_key: String,
    session_uuid: String,
    sanitized_cwd: String,
    machine_id: String,
    title: Option<String>,
    event_count: i64,
    turns_html: Vec<String>,
    /// Smallest `line_no` rendered; used as `before` cursor for the next "load older" request.
    oldest_cursor: Option<i64>,
}

#[derive(Template)]
#[template(path = "transcript_older.html")]
struct TranscriptOlderTpl {
    turns_html: Vec<String>,
    oldest_cursor: Option<i64>,
}

#[derive(Deserialize)]
pub struct OlderQuery {
    before: Option<i64>,
}

pub async fn transcript(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
) -> Response {
    let session = match sqlx::query(
        "SELECT session_uuid, sanitized_cwd, machine_id, event_count FROM sessions WHERE session_key = ?",
    )
    .bind(&session_key)
    .fetch_optional(state.pool())
    .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return (StatusCode::NOT_FOUND, "session not found").into_response(),
        Err(e) => return server_error(e),
    };

    let title: Option<String> = sqlx::query_scalar(
        "SELECT json_extract(CAST(raw AS TEXT), '$.aiTitle') FROM events
         WHERE session_key = ? AND kind = 'ai-title' ORDER BY line_no DESC LIMIT 1",
    )
    .bind(&session_key)
    .fetch_optional(state.pool())
    .await
    .ok()
    .flatten();

    let (turns_html, oldest_cursor) =
        match load_latest(&state, &session_key, PAGE_SIZE).await {
            Ok(v) => v,
            Err(e) => return server_error(e),
        };

    render(TranscriptTpl {
        session_key: session_key.clone(),
        session_uuid: session.try_get("session_uuid").unwrap_or_default(),
        sanitized_cwd: session.try_get("sanitized_cwd").unwrap_or_default(),
        machine_id: session.try_get("machine_id").unwrap_or_default(),
        title,
        event_count: session.try_get("event_count").unwrap_or(0),
        turns_html,
        oldest_cursor,
    })
}

pub async fn transcript_older(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
    Query(q): Query<OlderQuery>,
) -> Response {
    let Some(before) = q.before else {
        return (StatusCode::BAD_REQUEST, "missing ?before=N").into_response();
    };
    let (turns_html, oldest_cursor) =
        match load_older(&state, &session_key, before, OLDER_PAGE_SIZE).await {
            Ok(v) => v,
            Err(e) => return server_error(e),
        };
    render(TranscriptOlderTpl {
        turns_html,
        oldest_cursor,
    })
}

/// Load the most recent `limit` main events, returned in chronological order so the
/// renderer can just iterate top-to-bottom.
async fn load_latest(
    state: &AppState,
    session_key: &str,
    limit: i64,
) -> Result<(Vec<String>, Option<i64>), sqlx::Error> {
    let mut rows = sqlx::query(
        "SELECT line_no, byte_offset, timestamp, kind, agent_id, CAST(raw AS TEXT) AS raw
         FROM events
         WHERE session_key = ? AND is_sidechain = 0
         ORDER BY line_no DESC
         LIMIT ?",
    )
    .bind(session_key)
    .bind(limit)
    .fetch_all(state.pool())
    .await?;
    rows.reverse();
    let oldest_cursor = rows
        .first()
        .and_then(|r| r.try_get::<i64, _>("line_no").ok());

    let (side_rows, meta_rows) = load_session_extras(state, session_key).await?;
    let turns_html = build_turns_html(rows, side_rows, meta_rows);
    Ok((turns_html, oldest_cursor))
}

/// Load up to `limit` events with `line_no < before`, chronologically.
async fn load_older(
    state: &AppState,
    session_key: &str,
    before: i64,
    limit: i64,
) -> Result<(Vec<String>, Option<i64>), sqlx::Error> {
    let mut rows = sqlx::query(
        "SELECT line_no, byte_offset, timestamp, kind, agent_id, CAST(raw AS TEXT) AS raw
         FROM events
         WHERE session_key = ? AND is_sidechain = 0 AND line_no < ?
         ORDER BY line_no DESC
         LIMIT ?",
    )
    .bind(session_key)
    .bind(before)
    .bind(limit)
    .fetch_all(state.pool())
    .await?;
    rows.reverse();
    let oldest_cursor = rows
        .first()
        .and_then(|r| r.try_get::<i64, _>("line_no").ok());

    let (side_rows, meta_rows) = load_session_extras(state, session_key).await?;
    let turns_html = build_turns_html(rows, side_rows, meta_rows);
    Ok((turns_html, oldest_cursor))
}

async fn load_session_extras(
    state: &AppState,
    session_key: &str,
) -> Result<
    (
        Vec<sqlx::sqlite::SqliteRow>,
        Vec<sqlx::sqlite::SqliteRow>,
    ),
    sqlx::Error,
> {
    let side_rows = sqlx::query(
        "SELECT agent_id, line_no, timestamp, kind, CAST(raw AS TEXT) AS raw
         FROM events
         WHERE session_key = ? AND is_sidechain = 1
         ORDER BY agent_id ASC, line_no ASC",
    )
    .bind(session_key)
    .fetch_all(state.pool())
    .await?;
    let meta_rows = sqlx::query(
        "SELECT agent_file, agent_type, description FROM subagent_meta WHERE session_key = ?",
    )
    .bind(session_key)
    .fetch_all(state.pool())
    .await?;
    Ok((side_rows, meta_rows))
}

// ─── WebSocket: live event tail ───────────────────────────────────────────────

pub async fn transcript_ws(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| ws_loop(state, session_key, socket))
}

async fn ws_loop(state: AppState, session_key: String, mut socket: WebSocket) {
    let mut rx = state.bus().subscribe(&session_key);
    loop {
        tokio::select! {
            // Server-side: a new event arrived for this session — forward it.
            msg = rx.recv() => {
                match msg {
                    Ok(html) => {
                        if socket.send(Message::Text(html)).await.is_err() {
                            break;
                        }
                    }
                    // If the channel lagged we tell the client; it can refresh.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = socket.send(Message::Text(
                            "<!-- bus lagged; refresh for the latest -->".to_string()
                        )).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            // Client-side: pings / close.
            ws_msg = socket.recv() => {
                match ws_msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(p))) => {
                        if socket.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

// ─── Live status JSON ────────────────────────────────────────────────────────

#[derive(serde::Serialize, Default)]
struct LiveStatus {
    /// Pre-computed pill label: "working" | "idle" | "stale" | "exited" | "unknown".
    derived_status: String,
    /// Raw status from CC's sessions/<pid>.json (e.g. "waiting").
    status: String,
    pid: Option<i64>,
    started_at: Option<String>,
    updated_at: Option<String>,
    active_subagents: i64,
    background_tasks: i64,
    heartbeat_at: Option<String>,
}

pub async fn session_live(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
) -> Response {
    let row = match sqlx::query(
        "SELECT status, pid, started_at, updated_at, active_subagents,
                background_tasks, heartbeat_at
         FROM live_sessions WHERE session_key = ?",
    )
    .bind(&session_key)
    .fetch_optional(state.pool())
    .await
    {
        Ok(r) => r,
        Err(e) => return server_error(e),
    };
    let payload = match row {
        Some(r) => {
            let status: String = r.try_get("status").unwrap_or_default();
            let updated_at: Option<String> = r.try_get("updated_at").ok();
            let heartbeat_at: Option<String> = r.try_get("heartbeat_at").ok();
            let derived = derive_status(&status, updated_at.as_deref(), heartbeat_at.as_deref());
            LiveStatus {
                derived_status: derived,
                status,
                pid: r.try_get("pid").ok(),
                started_at: r.try_get("started_at").ok(),
                updated_at,
                active_subagents: r.try_get("active_subagents").unwrap_or(0),
                background_tasks: r.try_get("background_tasks").unwrap_or(0),
                heartbeat_at,
            }
        }
        None => LiveStatus {
            derived_status: "unknown".into(),
            status: "unknown".into(),
            ..Default::default()
        },
    };
    Json(payload).into_response()
}

fn derive_status(status: &str, updated_at: Option<&str>, heartbeat_at: Option<&str>) -> String {
    let now = chrono::Utc::now();
    let stale = match heartbeat_at.and_then(parse_text_to_epoch_secs) {
        Some(t) => (now.timestamp() - t) > 30,
        None => true,
    };
    if stale {
        return "stale".into();
    }
    let lc = status.to_ascii_lowercase();
    if lc == "exited" {
        return "exited".into();
    }
    if lc == "busy" || lc == "working" || lc == "running" {
        return "working".into();
    }
    if lc == "waiting" || lc == "idle" {
        return "idle".into();
    }
    // Fall back to the "recently written" heuristic for CC versions that don't carry
    // a busy/waiting distinction in sessions/<pid>.json.
    let working = match updated_at.and_then(parse_text_to_epoch_secs) {
        Some(t) => (now.timestamp() - t) < 5,
        None => false,
    };
    if working {
        "working".into()
    } else if status.is_empty() {
        "idle".into()
    } else {
        status.to_string()
    }
}

/// Parse either epoch-ms (e.g. "1779107797964"), epoch-seconds, or an ISO-8601
/// timestamp into Unix seconds. Best-effort; returns None on parse failure.
fn parse_text_to_epoch_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<i64>() {
        // Heuristic: anything bigger than year-3000 seconds is probably milliseconds.
        if n > 32_503_680_000 {
            return Some(n / 1000);
        }
        return Some(n);
    }
    // sqlite datetime('now') format: "YYYY-MM-DD HH:MM:SS"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp());
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    None
}

// ─── Search ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub team: Option<String>,
}

#[derive(Template)]
#[template(path = "search.html")]
struct SearchTpl {
    query: String,
    hits: Vec<Hit>,
}

/// /ui/search?q=... — server-rendered HTML. No auth in v1 (UI is open). All teams.
pub async fn search(State(state): State<AppState>, Query(q): Query<SearchQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let hits = match search::hybrid(state.pool(), state.embedder(), &q.q, q.team.as_deref(), limit)
        .await
    {
        Ok(h) => h,
        Err(e) => return server_error(e),
    };
    render(SearchTpl { query: q.q, hits })
}

/// /api/search?q=... — JSON. Auth-required, team-scoped to the X-Api-Key's team.
pub async fn api_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SearchQuery>,
) -> Response {
    let key = headers
        .get(HEADER_API_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let team = match auth::lookup(state.pool(), key).await {
        Ok(Some(ctx)) => ctx.team_id,
        Ok(None) => {
            return (StatusCode::UNAUTHORIZED, "invalid or missing api key").into_response();
        }
        Err(e) => return server_error(e),
    };
    let limit = q.limit.unwrap_or(25).clamp(1, 200);
    let hits = match search::hybrid(state.pool(), state.embedder(), &q.q, Some(&team), limit).await
    {
        Ok(h) => h,
        Err(e) => return server_error(e),
    };
    Json(hits).into_response()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

fn render<T: Template>(t: T) -> Response {
    match t.render() {
        Ok(s) => Html(s).into_response(),
        Err(e) => server_error(e),
    }
}

fn server_error<E: std::fmt::Display>(e: E) -> Response {
    tracing::error!(error = %e, "ui error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("internal error: {e}"),
    )
        .into_response()
}
