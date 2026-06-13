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
/// Per-drain batch size for the SSE row stream (`stream_rows`). Unrelated to the
/// one-shot page API's `size` cap (`MAX_SIZE`).
pub const PAGE_LIMIT: i64 = 500;
/// Upper bound on the `size` query parameter for the one-shot page API.
pub const MAX_SIZE: i64 = 1000;

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
///
/// `offset >= 0` returns events with `seq > offset` (cursor pagination).
/// `offset < 0` is the tail sentinel (`-1`): return the latest `size` events
/// without knowing the current cursor. `size` is clamped to `1..=MAX_SIZE`;
/// the dispatcher rejects out-of-range values with 400 before reaching here,
/// so the clamp is purely defensive (and guards the SQLite `LIMIT -1` =
/// "unlimited" footgun).
pub async fn page(
    state: &AppState,
    sid: &str,
    transcript_only: bool,
    offset: i64,
    size: i64,
    wait: u64,
) -> CoreResult<CoreResponse> {
    ensure_exists(state, sid).await?;
    let wait = wait.min(MAX_WAIT);
    let size = size.clamp(1, MAX_SIZE);
    let tail = offset < 0;
    let select = || async {
        match (tail, transcript_only) {
            (true, true) => events_store::select_messages_tail(&state.db, sid, size).await,
            (true, false) => events_store::select_events_tail(&state.db, sid, size).await,
            (false, true) => {
                events_store::select_messages_since(&state.db, sid, offset, size).await
            }
            (false, false) => events_store::select_events_since(&state.db, sid, offset, size).await,
        }
    };

    let mut rows = select().await.map_err(internal)?;
    if rows.is_empty() && wait > 0 {
        let notify = state.bus.subscribe(sid).await;
        let notified = notify.notified();
        tokio::pin!(notified);
        let _ = tokio::time::timeout(Duration::from_secs(wait), notified).await;
        rows = select().await.map_err(internal)?;
    }
    let next_cursor = rows
        .last()
        .map(|r| r.seq)
        .unwrap_or(if tail { 0 } else { offset });
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

/// The single choke point for emitting an event: append to the DB, wake local
/// long-poll/SSE waiters, and forward to Xpo-k (`session_event` + a
/// `status_update` when the derived status changed). Every former
/// `append_event` + `bus.notify` pair routes through here so forwarding can
/// never be forgotten.
pub async fn record(
    state: &AppState,
    sid: &str,
    kind: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<i64> {
    let seq = events_store::append_event(&state.db, sid, &events_store::now_iso(), kind, payload)
        .await?;
    state.bus.notify(sid).await;
    forward(state, sid, kind, payload).await;
    Ok(seq)
}

/// Forward an already-persisted event to Xpo-k and emit a status_update when
/// the derived status changed. Call this after `append_jsonl_event` (whose
/// atomic offset bump can't go through `record`).
pub async fn forward(state: &AppState, sid: &str, kind: &str, payload: &serde_json::Value) {
    state
        .uplink_send(pok_proto::WsMsg::SessionEvent {
            sid: sid.to_string(),
            event: pok_proto::EventEnvelope {
                kind: kind.to_string(),
                payload: payload.clone(),
            },
        })
        .await;
    push_status_if_changed(state, sid).await;
}

async fn push_status_if_changed(state: &AppState, sid: &str) {
    let ended_at = events_store::get_session(&state.db, sid)
        .await
        .ok()
        .flatten()
        .and_then(|s| s.ended_at);
    let latest = match events_store::latest_status_seqs(&state.db, sid).await {
        Ok(l) => l,
        Err(_) => return,
    };
    let (st, _) = crate::status::derive_status(&latest, ended_at.as_deref());
    let st = st.as_str().to_string();
    let changed = state
        .last_status
        .get(sid)
        .map(|v| *v != st)
        .unwrap_or(true);
    if changed {
        state.last_status.insert(sid.to_string(), st.clone());
        state
            .uplink_send(pok_proto::WsMsg::StatusUpdate {
                sid: sid.to_string(),
                status: st,
            })
            .await;
    }
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
