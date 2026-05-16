use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use po_k_core::Event;
use po_k_proto::{BatchHeader, IngestResponse, SubagentMetaRow, HEADER_API_KEY};
use sqlx::Acquire;

use crate::auth;
use crate::state::AppState;

pub async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let team_id = match authenticate(&state, &headers).await {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    // Parse NDJSON: first line = header, rest = events.
    let mut lines = body.split(|b| *b == b'\n');
    let Some(header_line) = lines.next() else {
        return bad_request("empty body", None);
    };
    let header: BatchHeader = match serde_json::from_slice(header_line) {
        Ok(h) => h,
        Err(e) => {
            return bad_request(&format!("invalid batch header: {e}"), Some(0));
        }
    };

    let mut events: Vec<Event> = Vec::with_capacity(header.count as usize);
    let mut line_no: u64 = 1;
    for raw in lines {
        if raw.is_empty() {
            line_no += 1;
            continue;
        }
        match serde_json::from_slice::<Event>(raw) {
            Ok(ev) => events.push(ev),
            Err(e) => {
                return bad_request(&format!("invalid event line: {e}"), Some(line_no));
            }
        }
        line_no += 1;
    }

    if events.len() as u64 != header.count {
        return bad_request(
            &format!(
                "header count {} != actual events {}",
                header.count,
                events.len()
            ),
            None,
        );
    }

    let mut conn = match state.pool().acquire().await {
        Ok(c) => c,
        Err(e) => return server_error(&format!("acquire conn: {e}")),
    };
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => return server_error(&format!("begin tx: {e}")),
    };

    // Touch the machine row.
    if let Err(e) = sqlx::query(
        "INSERT INTO machines (team_id, machine_id) VALUES (?, ?)
         ON CONFLICT(team_id, machine_id) DO UPDATE SET last_seen = datetime('now')",
    )
    .bind(&team_id)
    .bind(header.machine_id.as_str())
    .execute(&mut *tx)
    .await
    {
        return server_error(&format!("upsert machine: {e}"));
    }

    let mut accepted: u64 = 0;
    let mut duplicates: u64 = 0;
    // (session_key, kind, raw, timestamp) for events we accepted (not duplicates),
    // so we can publish them onto the bus after commit. Sidechain events aren't
    // published in v1 — live updates are main-session only.
    let mut to_publish: Vec<(String, String, String, Option<String>)> = Vec::new();

    for ev in &events {
        // Upsert session aggregate.
        let (sanitized_cwd, session_uuid) = split_session_path(&ev.file_relpath);
        if let Err(e) = sqlx::query(
            "INSERT INTO sessions (session_key, team_id, machine_id, sanitized_cwd, session_uuid, first_event_at, last_event_at, event_count)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0)
             ON CONFLICT(session_key) DO UPDATE SET
                first_event_at = COALESCE(MIN(first_event_at, excluded.first_event_at), first_event_at, excluded.first_event_at),
                last_event_at  = COALESCE(MAX(last_event_at,  excluded.last_event_at),  last_event_at,  excluded.last_event_at)",
        )
        .bind(ev.session_key.as_str())
        .bind(&team_id)
        .bind(header.machine_id.as_str())
        .bind(&sanitized_cwd)
        .bind(&session_uuid)
        .bind(&ev.timestamp)
        .bind(&ev.timestamp)
        .execute(&mut *tx)
        .await
        {
            return server_error(&format!("upsert session: {e}"));
        }

        let res = sqlx::query(
            "INSERT OR IGNORE INTO events
             (session_key, file_relpath, line_no, byte_offset, team_id, timestamp, kind, is_sidechain, agent_id, raw)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(ev.session_key.as_str())
        .bind(&ev.file_relpath)
        .bind(ev.line_no as i64)
        .bind(ev.byte_offset as i64)
        .bind(&team_id)
        .bind(&ev.timestamp)
        .bind(&ev.kind)
        .bind(ev.is_sidechain as i64)
        .bind(&ev.agent_id)
        .bind(ev.raw.as_bytes())
        .execute(&mut *tx)
        .await;

        match res {
            Ok(r) => {
                if r.rows_affected() == 1 {
                    accepted += 1;
                    // Mirror into fts5 only on first insert so the index stays in
                    // sync with the events table. INSERT OR IGNORE already handled
                    // dedupe above; here we just record the new row.
                    if let Err(e) = sqlx::query(
                        "INSERT INTO events_fts (session_key, file_relpath, line_no, team_id, raw)
                         VALUES (?, ?, ?, ?, ?)",
                    )
                    .bind(ev.session_key.as_str())
                    .bind(&ev.file_relpath)
                    .bind(ev.line_no as i64)
                    .bind(&team_id)
                    .bind(&ev.raw)
                    .execute(&mut *tx)
                    .await
                    {
                        return server_error(&format!("insert fts5: {e}"));
                    }
                    if !ev.is_sidechain {
                        to_publish.push((
                            ev.session_key.as_str().to_string(),
                            ev.kind.clone(),
                            ev.raw.clone(),
                            ev.timestamp.clone(),
                        ));
                    }
                } else {
                    duplicates += 1;
                }
            }
            Err(e) => return server_error(&format!("insert event: {e}")),
        }
    }

    // Recompute event_count per touched session in this batch (small N per batch).
    // For correctness over speed in M1; M2+ can incrementalize.
    let mut keys: Vec<&str> = events.iter().map(|e| e.session_key.as_str()).collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        if let Err(e) = sqlx::query(
            "UPDATE sessions SET event_count = (SELECT COUNT(*) FROM events WHERE session_key = ?) WHERE session_key = ?",
        )
        .bind(k)
        .bind(k)
        .execute(&mut *tx)
        .await
        {
            return server_error(&format!("recount session: {e}"));
        }
    }

    if let Err(e) = tx.commit().await {
        return server_error(&format!("commit: {e}"));
    }

    // Publish accepted events onto the live bus. Render each to HTML on the way
    // out so WebSocket subscribers can append it directly.
    for (session_key, kind, raw, ts) in &to_publish {
        if let Some(html) = crate::transcript::render_event_to_html(kind, raw, ts.as_deref()) {
            state.bus().publish(session_key, html);
        }
    }

    (
        StatusCode::OK,
        Json(IngestResponse::Ok { accepted, duplicates }),
    )
        .into_response()
}

fn bad_request(message: &str, rejected_line: Option<u64>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(IngestResponse::Error {
            message: message.to_string(),
            rejected_line,
        }),
    )
        .into_response()
}

fn server_error(message: &str) -> Response {
    tracing::error!(error = %message, "ingest error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(IngestResponse::Error {
            message: message.to_string(),
            rejected_line: None,
        }),
    )
        .into_response()
}

pub async fn ingest_subagent_meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _team_id = match authenticate(&state, &headers).await {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    // subagent_meta rows are joined to events via session_key, and session_key already
    // encodes team boundaries (collector ships under its own team's key). No explicit
    // team_id needed on these rows for M3.

    let mut rows: Vec<SubagentMetaRow> = Vec::new();
    let mut line_no: u64 = 0;
    for raw in body.split(|b| *b == b'\n') {
        if raw.is_empty() {
            line_no += 1;
            continue;
        }
        match serde_json::from_slice::<SubagentMetaRow>(raw) {
            Ok(row) => rows.push(row),
            Err(e) => return bad_request(&format!("invalid meta line: {e}"), Some(line_no)),
        }
        line_no += 1;
    }

    let mut conn = match state.pool().acquire().await {
        Ok(c) => c,
        Err(e) => return server_error(&format!("acquire conn: {e}")),
    };
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => return server_error(&format!("begin tx: {e}")),
    };
    let mut accepted = 0u64;
    for r in &rows {
        if let Err(e) = sqlx::query(
            "INSERT INTO subagent_meta (session_key, agent_file, agent_type, description)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(session_key, agent_file) DO UPDATE SET
                agent_type = excluded.agent_type,
                description = excluded.description",
        )
        .bind(&r.session_key)
        .bind(&r.agent_file)
        .bind(r.agent_type.as_deref())
        .bind(r.description.as_deref())
        .execute(&mut *tx)
        .await
        {
            return server_error(&format!("upsert meta: {e}"));
        }
        accepted += 1;
    }
    if let Err(e) = tx.commit().await {
        return server_error(&format!("commit: {e}"));
    }
    (
        StatusCode::OK,
        Json(IngestResponse::Ok {
            accepted,
            duplicates: 0,
        }),
    )
        .into_response()
}

/// Pull the API key from the request, look it up, and return the bound team_id.
/// On failure, returns a fully-rendered 401/500 response the caller can return as-is.
async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<String, Response> {
    let key = headers
        .get(HEADER_API_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    match auth::lookup(state.pool(), key).await {
        Ok(Some(ctx)) => Ok(ctx.team_id),
        Ok(None) => Err((
            StatusCode::UNAUTHORIZED,
            Json(IngestResponse::Error {
                message: "invalid or missing api key".into(),
                rejected_line: None,
            }),
        )
            .into_response()),
        Err(e) => Err(server_error(&format!("auth lookup failed: {e}"))),
    }
}

/// Split a file_relpath like `-workspace/<uuid>.jsonl` or
/// `-workspace/<uuid>/subagents/agent-<id>.jsonl` into (sanitized_cwd, session_uuid).
fn split_session_path(rel: &str) -> (String, String) {
    let mut it = rel.split('/');
    let cwd = it.next().unwrap_or("").to_string();
    let second = it.next().unwrap_or("");
    let uuid = second.strip_suffix(".jsonl").unwrap_or(second).to_string();
    (cwd, uuid)
}
