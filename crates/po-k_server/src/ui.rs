//! Server-rendered UI: project list, session list, transcript view.

use askama::Template;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::Row;

use crate::state::AppState;
use crate::transcript::build_turns_html;

const PAGE_SIZE: i64 = 200;

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
    next_cursor: Option<i64>,
    remaining: i64,
}

#[derive(Template)]
#[template(path = "transcript_page.html")]
struct TranscriptPageTpl {
    session_key: String,
    turns_html: Vec<String>,
    next_cursor: Option<i64>,
    remaining: i64,
}

#[derive(Deserialize)]
pub struct PageQuery {
    cursor: Option<i64>,
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

    let (turns_html, next_cursor, remaining) =
        match load_page(&state, &session_key, 0).await {
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
        next_cursor,
        remaining,
    })
}

pub async fn transcript_page(
    State(state): State<AppState>,
    Path(session_key): Path<String>,
    Query(q): Query<PageQuery>,
) -> Response {
    let cursor = q.cursor.unwrap_or(0);
    let (turns_html, next_cursor, remaining) =
        match load_page(&state, &session_key, cursor).await {
            Ok(v) => v,
            Err(e) => return server_error(e),
        };

    render(TranscriptPageTpl {
        session_key,
        turns_html,
        next_cursor,
        remaining,
    })
}

async fn load_page(
    state: &AppState,
    session_key: &str,
    cursor: i64,
) -> Result<(Vec<String>, Option<i64>, i64), sqlx::Error> {
    let mut main_rows = sqlx::query(
        "SELECT line_no, byte_offset, timestamp, kind, agent_id, CAST(raw AS TEXT) AS raw
         FROM events
         WHERE session_key = ? AND is_sidechain = 0 AND line_no >= ?
         ORDER BY line_no ASC
         LIMIT ?",
    )
    .bind(session_key)
    .bind(cursor)
    .bind(PAGE_SIZE + 1)
    .fetch_all(state.pool())
    .await?;

    let has_more = main_rows.len() as i64 > PAGE_SIZE;
    if has_more {
        main_rows.pop();
    }

    let next_cursor = if has_more {
        main_rows
            .last()
            .and_then(|r| r.try_get::<i64, _>("line_no").ok())
            .map(|ln| ln + 1)
    } else {
        None
    };

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

    let turns_html = build_turns_html(main_rows, side_rows, meta_rows);

    let remaining = if has_more {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE session_key = ? AND is_sidechain = 0 AND line_no >= ?",
        )
        .bind(session_key)
        .bind(next_cursor.unwrap_or(0))
        .fetch_one(state.pool())
        .await?
    } else {
        0
    };

    Ok((turns_html, next_cursor, remaining))
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
