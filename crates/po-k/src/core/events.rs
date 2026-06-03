//! Event querying: long-poll pages, cost aggregation, and the row stream that
//! backs both the SSE endpoint (Phase 1) and the WebSocket stream bridge
//! (Phase 2).

use futures::stream::Stream;
use serde_json::{json, Value};
use std::time::Duration;

use super::{internal, CoreError, CoreResult, CoreResponse};
use crate::events_store::{self, EventRow};
use crate::state::AppState;

pub const DEFAULT_WAIT: u64 = 30;
pub const MAX_WAIT: u64 = 60;
pub const PAGE_LIMIT: i64 = 500;

async fn ensure_exists(state: &AppState, sid: &str) -> CoreResult<()> {
    if events_store::get_session(&state.db, sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(CoreError::not_found(sid));
    }
    Ok(())
}

/// One long-poll page. `transcript_only` selects the `/messages` view.
pub async fn page(
    state: &AppState,
    sid: &str,
    transcript_only: bool,
    since: i64,
    wait: u64,
) -> CoreResult<CoreResponse> {
    ensure_exists(state, sid).await?;
    let wait = wait.min(MAX_WAIT);
    let select = |since: i64| async move {
        if transcript_only {
            events_store::select_messages_since(&state.db, sid, since, PAGE_LIMIT).await
        } else {
            events_store::select_events_since(&state.db, sid, since, PAGE_LIMIT).await
        }
    };

    let mut rows = select(since).await.map_err(internal)?;
    if rows.is_empty() && wait > 0 {
        let notify = state.bus.subscribe(sid).await;
        let notified = notify.notified();
        tokio::pin!(notified);
        let _ = tokio::time::timeout(Duration::from_secs(wait), notified).await;
        rows = select(since).await.map_err(internal)?;
    }
    let next_cursor = rows.last().map(|r| r.seq).unwrap_or(since);
    let key = if transcript_only { "messages" } else { "events" };
    Ok(CoreResponse::ok(json!({
        key: rows.iter().map(render_row).collect::<Vec<_>>(),
        "next_cursor": next_cursor,
    })))
}

pub async fn cost(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    ensure_exists(state, sid).await?;
    let rows = events_store::select_events_since(&state.db, sid, 0, 100_000)
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

    Ok(CoreResponse::ok(json!({
        "session_id": sid,
        "total_cost_usd": total_cost_usd,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
    })))
}

/// Infinite row stream from `since`, alternating DB drains and bus parks.
/// Dropping the consumer ends it. Each transport adds its own framing /
/// keepalive on top. Errors end the stream (logged by the caller).
pub fn stream_rows(
    state: AppState,
    sid: String,
    transcript_only: bool,
    since: i64,
) -> impl Stream<Item = EventRow> {
    async_stream::stream! {
        let notify = state.bus.subscribe(&sid).await;
        let mut cursor = since;
        loop {
            let rows = if transcript_only {
                events_store::select_messages_since(&state.db, &sid, cursor, PAGE_LIMIT).await
            } else {
                events_store::select_events_since(&state.db, &sid, cursor, PAGE_LIMIT).await
            };
            let rows = match rows {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(sid, error = %e, "event stream query failed");
                    break;
                }
            };
            for row in rows {
                cursor = row.seq;
                yield row;
            }
            let notified = notify.notified();
            tokio::pin!(notified);
            let _ = tokio::time::timeout(Duration::from_secs(30), notified).await;
        }
    }
}

/// Flatten an [`EventRow`] into the wire JSON: `{seq, ts, kind, ...payload}`.
pub fn render_row(r: &EventRow) -> Value {
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

/// SSE wire framing for one row: `event: <kind>\ndata: <json>\nid: <seq>\n\n`.
/// Used by the WebSocket stream bridge (Phase 2) which forwards these verbatim.
pub fn sse_frame(r: &EventRow) -> String {
    let data = serde_json::to_string(&render_row(r)).unwrap_or_else(|_| "{}".into());
    format!("event: {}\ndata: {}\nid: {}\n\n", r.kind, data, r.seq)
}
