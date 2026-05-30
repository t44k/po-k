//! Orchestrator control endpoints:
//!   - `GET /sessions/:id/status` — current derived CC status.
//!   - `GET /sessions/:id/wait?since=<seq>&timeout=<sec>` — block until CC is
//!     no longer working (idle / awaiting_input / ended), then return.
//!
//! Both derive status from the event stream via [`crate::status::derive_status`]
//! (see that module for the state model). `wait` is a long-poll: it parks on the
//! event bus and re-evaluates, returning the current status on timeout so the
//! caller can re-invoke (CC turns can outlast a single request).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

use crate::events_store::{self, Db};
use crate::http::events::{internal, not_found};
use crate::state::AppState;
use crate::status::{derive_status, Status};
use crate::zellij;

/// Default block before returning the current status; the caller re-invokes.
const WAIT_DEFAULT: u64 = 60;
/// Upper bound on a single `wait` request.
const WAIT_MAX: u64 = 600;
/// Re-query cadence inside `wait`. Bounds lost-wakeup latency: `bus.notify` uses
/// `notify_waiters()` (no stored permit), so a notify arriving between our DB
/// read and `notified().await` would otherwise be missed until the full
/// timeout. Re-checking at least this often recovers it.
const POLL_TICK: u64 = 5;

pub async fn status(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = events_store::get_session(&state.db, &sid)
        .await
        .map_err(internal)?
        .ok_or_else(|| not_found(&sid))?;
    let (st, deciding_seq) = current_status(&state.db, &sid, session.ended_at.as_deref()).await?;
    let deciding = deciding_event(&state.db, &sid, deciding_seq).await;
    Ok(Json(json!({
        "session_id": sid,
        "status": st.as_str(),
        "cursor": session.last_event_seq,
        "deciding_event": deciding,
        "ended_at": session.ended_at,
    })))
}

#[derive(Debug, Deserialize, Default)]
pub struct WaitQuery {
    /// Cursor the caller held when it sent its message. A stopped status only
    /// counts if its deciding event is newer than this — so a stale boundary
    /// from the previous turn can't end the wait prematurely.
    #[serde(default)]
    pub since: Option<i64>,
    /// Max seconds to block before returning the current status (`timed_out`).
    #[serde(default)]
    pub timeout: Option<u64>,
}

pub async fn wait(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if events_store::get_session(&state.db, &sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(not_found(&sid));
    }

    let since = q.since.unwrap_or(0);
    let total = q.timeout.unwrap_or(WAIT_DEFAULT).min(WAIT_MAX);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(total);
    // Subscribe once up front so a notify between iterations isn't lost.
    let notify = state.bus.subscribe(&sid).await;

    loop {
        let session = events_store::get_session(&state.db, &sid)
            .await
            .map_err(internal)?;
        let ended_at = session.as_ref().and_then(|s| s.ended_at.clone());
        let cursor = session.as_ref().map(|s| s.last_event_seq).unwrap_or(since);
        let (st, deciding_seq) = current_status(&state.db, &sid, ended_at.as_deref()).await?;

        // Only a stopped status whose deciding boundary is newer than `since`
        // satisfies the wait. `Ended` is always reported (terminal sessions
        // never un-end, and this also covers the kill path where `cc_exited` is
        // appended before `ended_at` is written).
        let satisfied = match st {
            Status::Ended => true,
            Status::Idle | Status::AwaitingInput => deciding_seq.map_or(false, |s| s > since),
            Status::Working => false,
        };
        if satisfied {
            let deciding = deciding_event(&state.db, &sid, deciding_seq).await;
            return Ok(Json(json!({
                "session_id": sid,
                "status": st.as_str(),
                "cursor": cursor,
                "deciding_event": deciding,
            })));
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let deciding = deciding_event(&state.db, &sid, deciding_seq).await;
            return Ok(Json(json!({
                "session_id": sid,
                "status": st.as_str(),
                "cursor": cursor,
                "deciding_event": deciding,
                "timed_out": true,
            })));
        }

        let tick = remaining.min(Duration::from_secs(POLL_TICK));
        let notified = notify.notified();
        tokio::pin!(notified);
        let _ = tokio::time::timeout(tick, notified).await;
    }
}

/// Return the visible content of the session's focused zellij pane. A
/// ground-truth view of CC's state that doesn't depend on the event stream
/// — useful for debugging mismatches with `/status` and as a cross-check in
/// tests. Requires a live session (in-memory `Registry`); ended sessions
/// have no useful pane.
pub async fn pane(
    State(state): State<AppState>,
    Path(sid): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let running = state
        .sessions
        .get(&sid)
        .await
        .ok_or_else(|| not_found(&sid))?;
    let content = zellij::read_focused_pane(&running.zellij_session)
        .await
        .map_err(internal)?;
    Ok(Json(json!({
        "session_id": sid,
        "zellij_session": running.zellij_session,
        "shows_prompt": zellij::shows_cc_prompt(&content),
        "content": content,
    })))
}

async fn current_status(
    db: &Db,
    sid: &str,
    ended_at: Option<&str>,
) -> Result<(Status, Option<i64>), (StatusCode, Json<Value>)> {
    let latest = events_store::latest_status_seqs(db, sid)
        .await
        .map_err(internal)?;
    Ok(derive_status(&latest, ended_at))
}

/// Fetch `{kind, seq, ts}` for the deciding event, or `null` when there isn't
/// one. Uses the existing `seq > since` query with `since = seq - 1`.
async fn deciding_event(db: &Db, sid: &str, seq: Option<i64>) -> Value {
    let Some(seq) = seq else { return Value::Null };
    match events_store::select_events_since(db, sid, seq - 1, 1).await {
        Ok(rows) => rows
            .into_iter()
            .next()
            .filter(|r| r.seq == seq)
            .map(|r| json!({ "kind": r.kind, "seq": r.seq, "ts": r.ts }))
            .unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}
