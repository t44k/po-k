//! Workflow endpoints (M17): correlation lookup plus the claim/release lease
//! that keeps autonomous continuation single-writer and bounded.
//!
//! These are Xpo-k-local (no po-k round trip), so a webhook-woken turn can
//! resolve its context and take the lease in two fast calls.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::state::XState;
use crate::workflow::{self, Claim, ClaimDenied};

type Resp = (StatusCode, Json<Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> Resp {
    (code, Json(json!({ "error": msg.into() })))
}

fn internal<E: std::fmt::Display>(e: E) -> Resp {
    err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn not_found(id: &str) -> Resp {
    err(StatusCode::NOT_FOUND, format!("workflow {id:?} not found"))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub subscriber: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    /// Find the workflow(s) belonging to a chat/topic — this is how the user's
    /// next message in the origin thread resolves the CC task it refers to.
    #[serde(default)]
    pub origin_chat_id: Option<String>,
    #[serde(default)]
    pub origin_thread_id: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /workflows` — list/lookup, newest first.
pub async fn list(State(st): State<XState>, Query(q): Query<ListQuery>) -> Resp {
    let filter = workflow::Filter {
        subscriber: q.subscriber,
        session_id: q.session_id,
        state: q.state,
        origin_chat_id: q.origin_chat_id,
        origin_thread_id: q.origin_thread_id,
    };
    match workflow::list(&st.db, &filter, q.limit.unwrap_or(20)).await {
        Ok(rows) => {
            let views: Vec<Value> = rows.iter().map(workflow::view).collect();
            (
                StatusCode::OK,
                Json(json!({ "workflows": views, "count": views.len() })),
            )
        }
        Err(e) => internal(e),
    }
}

/// `GET /workflows/{id}`
pub async fn get(State(st): State<XState>, Path(id): Path<String>) -> Resp {
    match workflow::get(&st.db, &id).await {
        Ok(Some(row)) => (StatusCode::OK, Json(workflow::view(&row))),
        Ok(None) => not_found(&id),
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct ClaimBody {
    /// Who is claiming — use something identifying the turn (notification id).
    pub owner: String,
    #[serde(default)]
    pub lease_secs: Option<i64>,
}

/// `POST /workflows/{id}/claim` — become the single writer for this task.
///
/// A refusal is a normal, expected answer (409), not an error: the woken turn
/// must then report/observe only and must NOT prompt the CC session. The body
/// always says why, so the agent can act on it.
pub async fn claim(
    State(st): State<XState>,
    Path(id): Path<String>,
    Json(body): Json<ClaimBody>,
) -> Resp {
    let owner = body.owner.trim();
    if owner.is_empty() {
        return err(StatusCode::BAD_REQUEST, "owner is required");
    }
    match workflow::claim(&st.db, &id, owner, body.lease_secs).await {
        Ok(None) => not_found(&id),
        Ok(Some(Claim::Granted(row))) => (
            StatusCode::OK,
            Json(json!({
                "claimed": true,
                "owner": owner,
                "workflow": workflow::view(&row),
            })),
        ),
        Ok(Some(Claim::Denied(reason))) => {
            let (why, detail) = match reason {
                ClaimDenied::Busy { owner, expires_at } => (
                    "busy",
                    json!({ "lease_owner": owner, "lease_expires_at": expires_at }),
                ),
                ClaimDenied::Exhausted { turns, max_turns } => (
                    "exhausted",
                    json!({ "turns": turns, "max_turns": max_turns }),
                ),
                ClaimDenied::Expired { deadline_at } => {
                    ("expired", json!({ "deadline_at": deadline_at }))
                }
                ClaimDenied::State(state) => ("state", json!({ "state": state })),
            };
            let current = workflow::get(&st.db, &id)
                .await
                .ok()
                .flatten()
                .map(|r| workflow::view(&r))
                .unwrap_or(Value::Null);
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "claimed": false,
                    "reason": why,
                    "detail": detail,
                    "workflow": current,
                    "guidance": "do NOT send pok_prompt for this session; report or \
                                 observe only, and leave the notification unacked if \
                                 nothing was handled",
                })),
            )
        }
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct ReleaseBody {
    pub owner: String,
    /// continued | waiting_for_human | done | failed | noop
    pub outcome: String,
    #[serde(default)]
    pub note: Option<String>,
}

/// `POST /workflows/{id}/release` — hand back the lease and record the outcome.
pub async fn release(
    State(st): State<XState>,
    Path(id): Path<String>,
    Json(body): Json<ReleaseBody>,
) -> Resp {
    let owner = body.owner.trim();
    if owner.is_empty() {
        return err(StatusCode::BAD_REQUEST, "owner is required");
    }
    match workflow::release(
        &st.db,
        &id,
        owner,
        body.outcome.trim(),
        body.note.as_deref(),
    )
    .await
    {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "workflow": workflow::view(&row) })),
        ),
        Ok(None) => not_found(&id),
        // A wrong owner or unknown outcome is the caller's mistake, not a 500.
        Err(e) => err(StatusCode::CONFLICT, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
pub struct ResumeBody {
    #[serde(default)]
    pub note: Option<String>,
}

/// `POST /workflows/{id}/resume` — the human answered.
///
/// Moves `waiting_for_human` back to `active` so autonomous turns may run again.
/// A no-op on every other state, so a stray resume cannot revive a finished or
/// budget-exhausted workflow.
pub async fn resume(
    State(st): State<XState>,
    Path(id): Path<String>,
    Json(body): Json<ResumeBody>,
) -> Resp {
    match workflow::resume(&st.db, &id, body.note.as_deref()).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "resumed": row.state == workflow::STATE_ACTIVE,
                "workflow": workflow::view(&row),
            })),
        ),
        Ok(None) => not_found(&id),
        Err(e) => internal(e),
    }
}
