//! Event stream endpoints:
//!   - `GET /sessions/:id/events?since=<seq>&wait=<sec>` — cursor long-poll.
//!   - `GET /sessions/:id/events/stream` — SSE: one `event: <kind>\ndata: <json>` per row.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use futures::stream::Stream;
use serde::Deserialize;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use crate::events_store::{self, EventRow};
use crate::state::AppState;

const DEFAULT_WAIT: u64 = 30;
const MAX_WAIT: u64 = 60;
const PAGE_LIMIT: i64 = 500;

#[derive(Debug, Deserialize, Default)]
pub struct PollQuery {
    #[serde(default)]
    pub since: Option<i64>,
    #[serde(default)]
    pub wait: Option<u64>,
}

pub async fn poll(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<PollQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Existence is best-effort: the session may have ended but events are
    // still readable from the DB. Treat unknown sid as 404 only if the DB
    // confirms it doesn't exist.
    if events_store::get_session(&state.db, &sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(not_found(&sid));
    }

    let since = q.since.unwrap_or(0);
    let wait = q.wait.unwrap_or(DEFAULT_WAIT).min(MAX_WAIT);
    let mut rows = events_store::select_events_since(&state.db, &sid, since, PAGE_LIMIT)
        .await
        .map_err(internal)?;

    if rows.is_empty() && wait > 0 {
        let notify = state.bus.subscribe(&sid).await;
        let notified = notify.notified();
        tokio::pin!(notified);
        let _ = tokio::time::timeout(Duration::from_secs(wait), notified).await;
        rows = events_store::select_events_since(&state.db, &sid, since, PAGE_LIMIT)
            .await
            .map_err(internal)?;
    }

    let next_cursor = rows.last().map(|r| r.seq).unwrap_or(since);
    Ok(Json(json!({
        "events": rows.iter().map(render_row).collect::<Vec<_>>(),
        "next_cursor": next_cursor,
    })))
}

pub async fn stream(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<PollQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    if events_store::get_session(&state.db, &sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(not_found(&sid));
    }

    let notify = state.bus.subscribe(&sid).await;
    let start = q.since.unwrap_or(0);

    // We assemble the stream by alternating between draining new events from
    // the DB and waiting on the `Notify`. To keep proxies happy we also send a
    // keepalive comment every 15s.
    let s = async_stream::try_stream! {
        let mut cursor = start;
        loop {
            let rows = events_store::select_events_since(&state.db, &sid, cursor, PAGE_LIMIT)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            for row in &rows {
                let value = render_row(row);
                let ev = Event::default()
                    .event(row.kind.as_str())
                    .data(serde_json::to_string(&value).unwrap_or_else(|_| "{}".into()))
                    .id(row.seq.to_string());
                cursor = row.seq;
                yield ev;
            }
            // Park until the next event lands or 30s elapses (then loop to re-poll).
            let notified = notify.notified();
            tokio::pin!(notified);
            let _ = tokio::time::timeout(Duration::from_secs(30), notified).await;
        }
    };

    // try_stream! yields Result<Event, io::Error>; fold the error into a
    // best-effort Infallible stream by tracing and dropping on error.
    let mapped = s.filter_map(|res: Result<Event, std::io::Error>| match res {
        Ok(ev) => Some(Ok::<Event, Infallible>(ev)),
        Err(e) => {
            tracing::warn!(error = %e, "sse stream error");
            None
        }
    });

    // Heartbeat every 15s; tower handles back-pressure.
    let heartbeat = IntervalStream::new(tokio::time::interval(Duration::from_secs(15)))
        .map(|_| Ok::<Event, Infallible>(Event::default().comment("keepalive")));

    let merged = mapped.merge(heartbeat);
    Ok(Sse::new(merged).keep_alive(KeepAlive::default()))
}

pub async fn cost(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if events_store::get_session(&state.db, &sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(not_found(&sid));
    }

    let rows = events_store::select_events_since(&state.db, &sid, 0, 100_000)
        .await
        .map_err(internal)?;

    let mut total_cost_usd: f64 = 0.0;
    let mut input_tokens: u64 = 0;
    let mut output_tokens: u64 = 0;
    let mut cache_creation_input_tokens: u64 = 0;
    let mut cache_read_input_tokens: u64 = 0;

    for row in &rows {
        if row.kind != "turn_end" {
            continue;
        }
        if let Some(c) = row.payload.get("total_cost_usd").and_then(|v| v.as_f64()) {
            total_cost_usd += c;
        }
        if let Some(u) = row.payload.get("usage") {
            input_tokens += u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
            output_tokens += u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
            cache_creation_input_tokens += u
                .get("cache_creation_input_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            cache_read_input_tokens += u
                .get("cache_read_input_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
        }
    }

    Ok(Json(json!({
        "session_id": sid,
        "total_cost_usd": total_cost_usd,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
    })))
}

fn render_row(r: &EventRow) -> Value {
    let mut out = json!({
        "seq": r.seq,
        "ts": r.ts,
        "kind": r.kind,
    });
    if let Value::Object(ref mut map) = out {
        if let Value::Object(payload_map) = &r.payload {
            for (k, v) in payload_map {
                map.entry(k.clone()).or_insert_with(|| v.clone());
            }
        } else {
            map.insert("payload".to_string(), r.payload.clone());
        }
    }
    out
}

fn not_found(sid: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("session {sid} not found") })),
    )
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("{e}") })),
    )
}

// silence unused-import warnings for axum traits not consumed in non-test paths
#[allow(dead_code)]
fn _unused(_: axum::response::Response) -> impl IntoResponse {
    StatusCode::OK
}
