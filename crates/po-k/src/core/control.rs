//! Derived status, wait, and live pane content.

use serde_json::{json, Value};
use std::time::Duration;

use super::{internal, CoreError, CoreResponse, CoreResult};
use crate::events_store::{self, Db};
use crate::state::AppState;
use crate::status::{derive_status, Status};
use crate::zellij;

const WAIT_DEFAULT: u64 = 60;
const WAIT_MAX: u64 = 600;
const POLL_TICK: u64 = 5;

pub async fn status(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    let session = events_store::get_session(&state.db, sid)
        .await
        .map_err(internal)?
        .ok_or_else(|| CoreError::not_found(sid))?;
    let (st, deciding_seq) = current_status(&state.db, sid, session.ended_at.as_deref()).await?;
    let deciding = deciding_event(&state.db, sid, deciding_seq).await;
    Ok(CoreResponse::ok(json!({
        "session_id": sid,
        "status": st.as_str(),
        "cursor": session.last_event_seq,
        "deciding_event": deciding,
        "ended_at": session.ended_at,
    })))
}

pub async fn wait(
    state: &AppState,
    sid: &str,
    since: i64,
    timeout: u64,
) -> CoreResult<CoreResponse> {
    if events_store::get_session(&state.db, sid)
        .await
        .map_err(internal)?
        .is_none()
    {
        return Err(CoreError::not_found(sid));
    }

    let total = timeout.min(WAIT_MAX);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(total);
    let notify = state.bus.subscribe(sid).await;

    loop {
        let session = events_store::get_session(&state.db, sid)
            .await
            .map_err(internal)?;
        let ended_at = session.as_ref().and_then(|s| s.ended_at.clone());
        let cursor = session.as_ref().map(|s| s.last_event_seq).unwrap_or(since);
        let (st, deciding_seq) = current_status(&state.db, sid, ended_at.as_deref()).await?;

        let satisfied = match st {
            Status::Ended => true,
            Status::Idle | Status::AwaitingInput => deciding_seq.is_some_and(|s| s > since),
            Status::Working => false,
        };
        if satisfied {
            let deciding = deciding_event(&state.db, sid, deciding_seq).await;
            return Ok(CoreResponse::ok(json!({
                "session_id": sid,
                "status": st.as_str(),
                "cursor": cursor,
                "deciding_event": deciding,
            })));
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let deciding = deciding_event(&state.db, sid, deciding_seq).await;
            return Ok(CoreResponse::ok(json!({
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

pub fn wait_defaults(timeout: Option<u64>) -> u64 {
    timeout.unwrap_or(WAIT_DEFAULT)
}

pub async fn pane(state: &AppState, sid: &str) -> CoreResult<CoreResponse> {
    let running = state
        .sessions
        .get(sid)
        .await
        .ok_or_else(|| CoreError::not_found(sid))?;
    let content = zellij::read_focused_pane(&running.zellij_session)
        .await
        .map_err(internal)?;
    Ok(CoreResponse::ok(json!({
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
) -> CoreResult<(Status, Option<i64>)> {
    let latest = events_store::latest_status_seqs(db, sid)
        .await
        .map_err(internal)?;
    Ok(derive_status(&latest, ended_at))
}

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
