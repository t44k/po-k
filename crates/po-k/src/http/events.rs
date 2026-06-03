//! Event stream endpoints — long-poll adapters over [`crate::core::events`]
//! plus SSE wrappers around its row stream.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures::stream::Stream;
use serde::Deserialize;
use serde_json::Value;
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use crate::core::events::{render_row, stream_rows, DEFAULT_WAIT};
use crate::state::AppState;

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
) -> (StatusCode, Json<Value>) {
    let since = q.since.unwrap_or(0);
    let wait = q.wait.unwrap_or(DEFAULT_WAIT);
    crate::http::adapt(crate::core::events::page(&state, &sid, false, since, wait).await)
}

pub async fn messages_poll(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<PollQuery>,
) -> (StatusCode, Json<Value>) {
    let since = q.since.unwrap_or(0);
    let wait = q.wait.unwrap_or(DEFAULT_WAIT);
    crate::http::adapt(crate::core::events::page(&state, &sid, true, since, wait).await)
}

pub async fn cost(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> (StatusCode, Json<Value>) {
    crate::http::adapt(crate::core::events::cost(&state, &sid).await)
}

pub async fn stream(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<PollQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    sse(state, sid, q, false).await
}

pub async fn messages_stream(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<PollQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    sse(state, sid, q, true).await
}

async fn sse(
    state: AppState,
    sid: String,
    q: PollQuery,
    transcript_only: bool,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let exists = crate::events_store::get_session(&state.db, &sid)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("{e}") })),
            )
        })?;
    if exists.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("session {sid} not found") })),
        ));
    }
    let start = q.since.unwrap_or(0);
    let rows = stream_rows(state, sid, transcript_only, start).map(|row| {
        let value = render_row(&row);
        Ok::<Event, Infallible>(
            Event::default()
                .event(row.kind.as_str())
                .data(serde_json::to_string(&value).unwrap_or_else(|_| "{}".into()))
                .id(row.seq.to_string()),
        )
    });
    let heartbeat = IntervalStream::new(tokio::time::interval(Duration::from_secs(15)))
        .map(|_| Ok::<Event, Infallible>(Event::default().comment("keepalive")));
    Ok(Sse::new(rows.merge(heartbeat)).keep_alive(KeepAlive::default()))
}
