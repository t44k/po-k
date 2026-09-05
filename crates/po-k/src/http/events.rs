//! Event endpoints: long-poll pages, cost, and SSE streams.

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::convert::Infallible;
use std::time::Duration;

use super::query::{page_params, since_param};
use crate::core::events::{render_row, stream_rows};
use crate::state::AppState;

async fn page(state: AppState, sid: String, q: Option<String>, transcript_only: bool) -> (StatusCode, Json<Value>) {
    match page_params(q.as_deref().unwrap_or("")) {
        Ok((offset, size, wait, follow)) => {
            super::adapt(crate::core::events::page(&state, &sid, transcript_only, offset, size, wait, follow).await)
        }
        Err(e) => super::adapt_err(e),
    }
}

pub async fn poll(State(state): State<AppState>, Path(sid): Path<String>, RawQuery(q): RawQuery) -> (StatusCode, Json<Value>) {
    page(state, sid, q, false).await
}

pub async fn messages_poll(State(state): State<AppState>, Path(sid): Path<String>, RawQuery(q): RawQuery) -> (StatusCode, Json<Value>) {
    page(state, sid, q, true).await
}

pub async fn cost(State(state): State<AppState>, Path(sid): Path<String>) -> (StatusCode, Json<Value>) {
    super::adapt(crate::core::events::cost(&state, &sid).await)
}

type SseResult = Result<Response, (StatusCode, Json<Value>)>;

pub async fn stream(State(state): State<AppState>, Path(sid): Path<String>, RawQuery(q): RawQuery) -> SseResult {
    sse(state, sid, q, false).await
}

pub async fn messages_stream(State(state): State<AppState>, Path(sid): Path<String>, RawQuery(q): RawQuery) -> SseResult {
    sse(state, sid, q, true).await
}

async fn sse(state: AppState, sid: String, q: Option<String>, transcript_only: bool) -> SseResult {
    let exists = crate::events_store::get_session(&state.db, &sid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": format!("{e}") }))))?;
    if exists.is_none() {
        return Err((StatusCode::NOT_FOUND, Json(json!({ "error": format!("session {sid} not found") }))));
    }
    let since = since_param(q.as_deref().unwrap_or(""));
    let rows: futures::stream::BoxStream<'static, Result<Event, Infallible>> = stream_rows(state, sid, transcript_only, since)
        .map(|row| {
            let value = render_row(&row);
            Ok::<Event, Infallible>(
                Event::default()
                    .event(row.kind.as_str())
                    .data(serde_json::to_string(&value).unwrap_or_else(|_| "{}".into()))
                    .id(row.seq.to_string()),
            )
        })
        .boxed();
    Ok(Sse::new(rows)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("keepalive"))
        .into_response())
}

#[allow(dead_code)]
fn _stream_type_check(s: impl Stream<Item = Result<Event, Infallible>>) -> impl Stream<Item = Result<Event, Infallible>> {
    s
}
