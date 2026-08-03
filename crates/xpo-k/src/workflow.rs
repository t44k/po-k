//! Workflows: the durable join between a CC task, the chat thread that asked
//! for it, and the bounded run of autonomous Hermes turns that drive it (M17).
//!
//! A webhook-woken Hermes turn is deliberately a *fresh, isolated* session —
//! that is what keeps it from interrupting the user's conversation. The cost is
//! that it starts with no memory: it does not know which Zulip topic asked, what
//! the task was, how many autonomous turns already ran, or whether another turn
//! is mid-flight on the same CC session. A workflow row is the state those turns
//! share.
//!
//! What it guarantees:
//!
//! * **Correlation.** `(subscriber, sid)` is unique, so every notification for a
//!   CC task resolves to one workflow id, and that row carries the opaque
//!   `origin` (chat/topic/user) needed to report back. Lookups work in both
//!   directions: by CC session, and by origin chat/thread so the user's *next*
//!   Zulip message can find the task it belongs to.
//! * **Single writer.** A turn must `claim` the lease before it prompts the CC
//!   session. A concurrent turn (duplicate push, cron fallback racing the
//!   webhook) fails the claim and must not prompt. This is the mechanism that
//!   prevents two `pok_prompt` calls landing in one CC session.
//! * **Bounded autonomy.** `turns` counts accepted prompts; `max_turns` and
//!   `deadline_at` are hard stops. When either is hit the workflow moves to a
//!   terminal state and further claims are refused, so a CC↔Hermes ping-pong
//!   cannot run away.
//! * **Explicit waiting.** `waiting_for_human` is a first-class state: the turn
//!   posted a question and stopped without acking. The user's reply resumes the
//!   workflow from the Zulip side.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::store::{now_epoch, now_iso, Db};

/// Autonomous turns allowed per workflow unless the caller overrides it.
pub const DEFAULT_MAX_TURNS: i64 = 8;
pub const MAX_MAX_TURNS: i64 = 100;
/// Wall-clock budget for autonomy, from creation.
pub const DEFAULT_BUDGET_SECS: i64 = 6 * 3600;
pub const MAX_BUDGET_SECS: i64 = 7 * 24 * 3600;
/// How long a claimed lease is valid before another turn may steal it. Longer
/// than a plausible turn, short enough that a crashed turn unblocks the task.
pub const DEFAULT_LEASE_SECS: i64 = 900;
pub const MAX_LEASE_SECS: i64 = 3600;

pub const STATE_ACTIVE: &str = "active";
pub const STATE_WAITING: &str = "waiting_for_human";
pub const STATE_DONE: &str = "done";
pub const STATE_FAILED: &str = "failed";
pub const STATE_EXHAUSTED: &str = "exhausted";
pub const STATE_EXPIRED: &str = "expired";

/// States from which no further autonomous turn may run.
pub fn is_terminal(state: &str) -> bool {
    matches!(
        state,
        STATE_DONE | STATE_FAILED | STATE_EXHAUSTED | STATE_EXPIRED
    )
}

/// Outcomes a turn may report when releasing the lease.
pub const OUTCOME_CONTINUED: &str = "continued";
pub const OUTCOME_WAITING: &str = "waiting_for_human";
pub const OUTCOME_DONE: &str = "done";
pub const OUTCOME_FAILED: &str = "failed";
pub const OUTCOME_NOOP: &str = "noop";

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRow {
    pub id: String,
    pub subscriber: String,
    pub session_id: String,
    pub origin: Value,
    pub state: String,
    pub turns: i64,
    pub max_turns: i64,
    pub deadline_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_note: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

type WfTuple = (
    String,
    String,
    String,
    Option<String>,
    String,
    i64,
    i64,
    i64,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
    String,
);

const WF_COLS: &str = "id, subscriber, sid, origin, state, turns, max_turns, deadline_at, \
     lease_owner, lease_expires_at, last_note, created_at, updated_at";

fn row_from(t: WfTuple) -> WorkflowRow {
    let (
        id,
        subscriber,
        sid,
        origin,
        state,
        turns,
        max_turns,
        deadline_at,
        lease_owner,
        lease_expires_at,
        last_note,
        created_at,
        updated_at,
    ) = t;
    WorkflowRow {
        id,
        subscriber,
        session_id: sid,
        origin: origin
            .and_then(|o| serde_json::from_str(&o).ok())
            .unwrap_or_else(|| json!({})),
        state,
        turns,
        max_turns,
        deadline_at,
        lease_owner,
        lease_expires_at,
        last_note,
        created_at,
        updated_at,
    }
}

/// Find-or-create the workflow for one `(subscriber, CC session)` pair.
///
/// Idempotent by design: every `pok_subscribe` for the same task lands on the
/// same workflow id, which is what makes the id usable as the correlation key in
/// notifications and webhook envelopes. An existing row keeps its counters; a
/// non-empty `origin` refreshes the stored one (the user may have moved the
/// conversation to a new topic), and a terminal workflow is reopened as
/// `active` so a re-subscribe can legitimately start a new run.
pub async fn ensure(
    db: &Db,
    subscriber: &str,
    sid: &str,
    origin: Option<&str>,
    max_turns: Option<i64>,
    budget_secs: Option<i64>,
) -> Result<WorkflowRow> {
    if let Some(existing) = get_by_task(db, subscriber, sid).await? {
        let reopen = is_terminal(&existing.state);
        if origin.is_some() || reopen {
            sqlx::query(
                r#"UPDATE workflows
                   SET origin = COALESCE(?1, origin),
                       state = CASE WHEN ?2 THEN ?3 ELSE state END,
                       turns = CASE WHEN ?2 THEN 0 ELSE turns END,
                       deadline_at = CASE WHEN ?2 THEN ?4 ELSE deadline_at END,
                       updated_at = ?5
                   WHERE id = ?6"#,
            )
            .bind(origin)
            .bind(reopen)
            .bind(STATE_ACTIVE)
            .bind(now_epoch() + clamp_budget(budget_secs))
            .bind(now_iso())
            .bind(&existing.id)
            .execute(db)
            .await
            .context("UPDATE workflow on re-subscribe")?;
            return Ok(get(db, &existing.id).await?.unwrap_or(existing));
        }
        return Ok(existing);
    }

    let now = now_iso();
    let row = WorkflowRow {
        id: format!("wf-{}", Uuid::new_v4()),
        subscriber: subscriber.to_string(),
        session_id: sid.to_string(),
        origin: origin
            .and_then(|o| serde_json::from_str(o).ok())
            .unwrap_or_else(|| json!({})),
        state: STATE_ACTIVE.to_string(),
        turns: 0,
        max_turns: max_turns
            .unwrap_or(DEFAULT_MAX_TURNS)
            .clamp(1, MAX_MAX_TURNS),
        deadline_at: now_epoch() + clamp_budget(budget_secs),
        lease_owner: None,
        lease_expires_at: None,
        last_note: None,
        created_at: now.clone(),
        updated_at: now,
    };
    // Another request may have created the same task concurrently; the unique
    // index turns that into a no-op and we return the winner.
    sqlx::query(
        r#"INSERT OR IGNORE INTO workflows
             (id, subscriber, sid, origin, state, turns, max_turns, deadline_at,
              lease_owner, lease_expires_at, last_note, created_at, updated_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,NULL,NULL,?9,?10)"#,
    )
    .bind(&row.id)
    .bind(&row.subscriber)
    .bind(&row.session_id)
    .bind(origin)
    .bind(&row.state)
    .bind(row.turns)
    .bind(row.max_turns)
    .bind(row.deadline_at)
    .bind(&row.created_at)
    .bind(&row.updated_at)
    .execute(db)
    .await
    .context("INSERT INTO workflows")?;
    Ok(get_by_task(db, subscriber, sid).await?.unwrap_or(row))
}

fn clamp_budget(secs: Option<i64>) -> i64 {
    secs.unwrap_or(DEFAULT_BUDGET_SECS)
        .clamp(60, MAX_BUDGET_SECS)
}

pub async fn get(db: &Db, id: &str) -> Result<Option<WorkflowRow>> {
    let row: Option<WfTuple> =
        sqlx::query_as(&format!("SELECT {WF_COLS} FROM workflows WHERE id = ?1"))
            .bind(id)
            .fetch_optional(db)
            .await
            .context("SELECT workflow")?;
    Ok(row.map(row_from))
}

pub async fn get_by_task(db: &Db, subscriber: &str, sid: &str) -> Result<Option<WorkflowRow>> {
    let row: Option<WfTuple> = sqlx::query_as(&format!(
        "SELECT {WF_COLS} FROM workflows WHERE subscriber = ?1 AND sid = ?2"
    ))
    .bind(subscriber)
    .bind(sid)
    .fetch_optional(db)
    .await
    .context("SELECT workflow by task")?;
    Ok(row.map(row_from))
}

/// Query filter for `GET /workflows`.
#[derive(Debug, Default, Clone)]
pub struct Filter {
    pub subscriber: Option<String>,
    pub session_id: Option<String>,
    pub state: Option<String>,
    /// Match `origin.chat_id` exactly — this is how a Zulip turn finds the
    /// workflow that belongs to the topic the user just replied in.
    pub origin_chat_id: Option<String>,
    /// Match `origin.thread_id` exactly (Zulip topic).
    pub origin_thread_id: Option<String>,
}

/// List workflows, newest first. Origin matching is done in Rust rather than
/// with SQL JSON functions so it works on any SQLite build.
pub async fn list(db: &Db, f: &Filter, limit: i64) -> Result<Vec<WorkflowRow>> {
    let rows: Vec<WfTuple> = sqlx::query_as(&format!(
        r#"SELECT {WF_COLS} FROM workflows
           WHERE (?1 IS NULL OR subscriber = ?1)
             AND (?2 IS NULL OR sid = ?2)
             AND (?3 IS NULL OR state = ?3)
           ORDER BY updated_at DESC, rowid DESC"#
    ))
    .bind(f.subscriber.as_deref())
    .bind(f.session_id.as_deref())
    .bind(f.state.as_deref())
    .fetch_all(db)
    .await
    .context("SELECT workflows")?;

    let want = |row: &WorkflowRow| -> bool {
        let matches = |key: &str, want: &Option<String>| match want {
            None => true,
            Some(v) => row.origin.get(key).and_then(|x| x.as_str()) == Some(v.as_str()),
        };
        matches("chat_id", &f.origin_chat_id) && matches("thread_id", &f.origin_thread_id)
    };
    Ok(rows
        .into_iter()
        .map(row_from)
        .filter(want)
        .take(limit.clamp(1, 200) as usize)
        .collect())
}

/// Why a claim was refused. The caller turns this into an instruction for the
/// woken agent, so each variant has to be actionable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimDenied {
    /// Another turn holds the lease — do not prompt the CC session.
    Busy { owner: String, expires_at: i64 },
    /// Turn budget spent.
    Exhausted { turns: i64, max_turns: i64 },
    /// Wall-clock budget spent.
    Expired { deadline_at: i64 },
    /// The workflow already finished (or is waiting on a human).
    State(String),
}

/// `Granted` is boxed so the enum stays small: a denial is the common answer on
/// a busy task and should not carry a full row's worth of stack.
#[derive(Debug, Clone)]
pub enum Claim {
    Granted(Box<WorkflowRow>),
    Denied(ClaimDenied),
}

/// Try to become the single writer for this workflow.
///
/// Bounds are enforced *before* the lease is granted and are persisted when they
/// trip, so an exhausted or expired workflow stays refused across restarts.
pub async fn claim(
    db: &Db,
    id: &str,
    owner: &str,
    lease_secs: Option<i64>,
) -> Result<Option<Claim>> {
    let Some(row) = get(db, id).await? else {
        return Ok(None);
    };
    let now = now_epoch();

    if is_terminal(&row.state) {
        return Ok(Some(Claim::Denied(ClaimDenied::State(row.state))));
    }
    if row.deadline_at <= now {
        set_state(db, id, STATE_EXPIRED, Some("wall-clock budget spent")).await?;
        return Ok(Some(Claim::Denied(ClaimDenied::Expired {
            deadline_at: row.deadline_at,
        })));
    }
    if row.turns >= row.max_turns {
        set_state(db, id, STATE_EXHAUSTED, Some("turn budget spent")).await?;
        return Ok(Some(Claim::Denied(ClaimDenied::Exhausted {
            turns: row.turns,
            max_turns: row.max_turns,
        })));
    }
    if row.state == STATE_WAITING {
        // A question is outstanding; only the human-reply path may move it on.
        return Ok(Some(Claim::Denied(ClaimDenied::State(row.state))));
    }

    let lease = lease_secs
        .unwrap_or(DEFAULT_LEASE_SECS)
        .clamp(30, MAX_LEASE_SECS);
    // Conditional update = the actual mutex. Only one caller can flip a free
    // (or expired) lease to its own name.
    let res = sqlx::query(
        r#"UPDATE workflows
           SET lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3
           WHERE id = ?4
             AND state IN (?5, ?6)
             AND (lease_owner IS NULL OR IFNULL(lease_expires_at, 0) <= ?7)"#,
    )
    .bind(owner)
    .bind(now + lease)
    .bind(now_iso())
    .bind(id)
    .bind(STATE_ACTIVE)
    .bind(STATE_WAITING)
    .bind(now)
    .execute(db)
    .await
    .context("UPDATE workflow claim")?;

    if res.rows_affected() == 0 {
        let cur = get(db, id).await?.unwrap_or(row);
        return Ok(Some(Claim::Denied(ClaimDenied::Busy {
            owner: cur.lease_owner.unwrap_or_default(),
            expires_at: cur.lease_expires_at.unwrap_or(0),
        })));
    }
    Ok(get(db, id).await?.map(|r| Claim::Granted(Box::new(r))))
}

/// Release the lease and record what the turn did.
///
/// `continued` is the only outcome that consumes turn budget — it means a new
/// prompt was accepted by the CC session, so the next notification is expected.
/// Releasing with the wrong owner is refused so a stale turn cannot clobber the
/// state of the turn that stole its lease.
pub async fn release(
    db: &Db,
    id: &str,
    owner: &str,
    outcome: &str,
    note: Option<&str>,
) -> Result<Option<WorkflowRow>> {
    let Some(row) = get(db, id).await? else {
        return Ok(None);
    };
    if let Some(current) = row.lease_owner.as_deref() {
        if current != owner {
            anyhow::bail!("lease is held by {current:?}, not {owner:?}");
        }
    }
    let (state, bump) = match outcome {
        OUTCOME_CONTINUED => (STATE_ACTIVE, 1),
        OUTCOME_WAITING => (STATE_WAITING, 0),
        OUTCOME_DONE => (STATE_DONE, 0),
        OUTCOME_FAILED => (STATE_FAILED, 0),
        OUTCOME_NOOP => (row.state.as_str(), 0),
        other => anyhow::bail!("unknown outcome {other:?}"),
    };
    let note: Option<String> = note.map(|n| n.chars().take(500).collect());
    sqlx::query(
        r#"UPDATE workflows
           SET lease_owner = NULL, lease_expires_at = NULL,
               state = ?1, turns = turns + ?2,
               last_note = COALESCE(?3, last_note), updated_at = ?4
           WHERE id = ?5"#,
    )
    .bind(state)
    .bind(bump)
    .bind(note)
    .bind(now_iso())
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE workflow release")?;

    // Trip the budget as soon as it is spent, so the refusal is durable rather
    // than recomputed on the next claim.
    if bump == 1 && row.turns + 1 >= row.max_turns {
        set_state(db, id, STATE_EXHAUSTED, Some("turn budget spent")).await?;
    }
    get(db, id).await
}

async fn set_state(db: &Db, id: &str, state: &str, note: Option<&str>) -> Result<()> {
    sqlx::query(
        r#"UPDATE workflows
           SET state = ?1, lease_owner = NULL, lease_expires_at = NULL,
               last_note = COALESCE(?2, last_note), updated_at = ?3
           WHERE id = ?4"#,
    )
    .bind(state)
    .bind(note)
    .bind(now_iso())
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE workflow state")?;
    Ok(())
}

/// Operator/agent-facing view with the derived numbers a turn needs to decide
/// whether it may continue.
pub fn view(row: &WorkflowRow) -> Value {
    let now = now_epoch();
    json!({
        "workflow_id": row.id,
        "subscriber": row.subscriber,
        "session_id": row.session_id,
        "origin": row.origin,
        "state": row.state,
        "turns": row.turns,
        "max_turns": row.max_turns,
        "turns_remaining": (row.max_turns - row.turns).max(0),
        "deadline_at": row.deadline_at,
        "seconds_remaining": (row.deadline_at - now).max(0),
        "terminal": is_terminal(&row.state),
        "lease_held": row.lease_expires_at.is_some_and(|e| e > now),
        "last_note": row.last_note,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    })
}

/// Move a `waiting_for_human` workflow back to `active`.
///
/// Only the human-reply path calls this: the user answered in the origin thread,
/// so autonomous turns may run again. A no-op on any other state, so a stray
/// resume cannot revive a finished or exhausted workflow.
pub async fn resume(db: &Db, id: &str, note: Option<&str>) -> Result<Option<WorkflowRow>> {
    let res = sqlx::query(
        r#"UPDATE workflows
           SET state = ?1, lease_owner = NULL, lease_expires_at = NULL,
               last_note = COALESCE(?2, last_note), updated_at = ?3
           WHERE id = ?4 AND state = ?5"#,
    )
    .bind(STATE_ACTIVE)
    .bind(note.map(|n| n.chars().take(500).collect::<String>()))
    .bind(now_iso())
    .bind(id)
    .bind(STATE_WAITING)
    .execute(db)
    .await
    .context("UPDATE workflow resume")?;
    if res.rows_affected() == 0 {
        return get(db, id).await;
    }
    get(db, id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;

    async fn fresh_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = store::open(&dir.path().join("x.db")).await.unwrap();
        (db, dir)
    }

    const ORIGIN: &str = r#"{"platform":"zulip","chat_id":"stream:eng","thread_id":"deploy-bug","user_name":"Tamas"}"#;

    #[tokio::test]
    async fn ensure_is_idempotent_per_task() {
        let (db, _d) = fresh_db().await;
        let a = ensure(&db, "hermes-1", "sess-1", Some(ORIGIN), None, None)
            .await
            .unwrap();
        let b = ensure(&db, "hermes-1", "sess-1", None, None, None)
            .await
            .unwrap();
        assert_eq!(a.id, b.id, "the same task must map to one workflow");
        assert_eq!(b.origin["thread_id"], "deploy-bug", "origin survives");
        // A different CC session, or a different subscriber, is a different task.
        let c = ensure(&db, "hermes-1", "sess-2", None, None, None)
            .await
            .unwrap();
        let d = ensure(&db, "other", "sess-1", None, None, None)
            .await
            .unwrap();
        assert_ne!(a.id, c.id);
        assert_ne!(a.id, d.id);
        assert_eq!(a.state, STATE_ACTIVE);
        assert_eq!(a.turns, 0);
        assert_eq!(a.max_turns, DEFAULT_MAX_TURNS);
    }

    #[tokio::test]
    async fn re_subscribe_refreshes_origin_and_reopens_a_finished_workflow() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", Some(ORIGIN), Some(2), None)
            .await
            .unwrap();
        release(&db, &wf.id, "owner-x", OUTCOME_DONE, Some("shipped"))
            .await
            .unwrap();
        assert_eq!(get(&db, &wf.id).await.unwrap().unwrap().state, STATE_DONE);

        let moved = r#"{"platform":"zulip","chat_id":"stream:eng","thread_id":"new-topic"}"#;
        let again = ensure(&db, "h", "s", Some(moved), None, None)
            .await
            .unwrap();
        assert_eq!(again.id, wf.id, "still the same task");
        assert_eq!(again.state, STATE_ACTIVE, "reopened for a new run");
        assert_eq!(again.turns, 0, "budget resets on reopen");
        assert_eq!(again.origin["thread_id"], "new-topic");
    }

    #[tokio::test]
    async fn claim_is_exclusive_until_release_or_expiry() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", Some(ORIGIN), None, None)
            .await
            .unwrap();

        let first = claim(&db, &wf.id, "turn-a", Some(60))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, Claim::Granted(_)));

        // A concurrent turn (duplicate push, cron racing the webhook) is refused.
        let second = claim(&db, &wf.id, "turn-b", Some(60))
            .await
            .unwrap()
            .unwrap();
        match second {
            Claim::Denied(ClaimDenied::Busy { owner, .. }) => assert_eq!(owner, "turn-a"),
            other => panic!("expected Busy, got {other:?}"),
        }

        // Releasing frees it for the next turn.
        release(&db, &wf.id, "turn-a", OUTCOME_CONTINUED, Some("prompted"))
            .await
            .unwrap();
        let third = claim(&db, &wf.id, "turn-b", Some(60))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(third, Claim::Granted(_)));
        assert_eq!(get(&db, &wf.id).await.unwrap().unwrap().turns, 1);
    }

    #[tokio::test]
    async fn an_expired_lease_can_be_stolen_so_a_crashed_turn_cannot_wedge_the_task() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", None, None, None).await.unwrap();
        claim(&db, &wf.id, "turn-a", Some(60)).await.unwrap();
        // Simulate the holder dying: age its lease.
        sqlx::query("UPDATE workflows SET lease_expires_at = ?1 WHERE id = ?2")
            .bind(now_epoch() - 1)
            .bind(&wf.id)
            .execute(&db)
            .await
            .unwrap();
        let stolen = claim(&db, &wf.id, "turn-b", Some(60))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(stolen, Claim::Granted(_)));
        // The abandoned turn can no longer mutate the workflow.
        assert!(release(&db, &wf.id, "turn-a", OUTCOME_DONE, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn turn_budget_is_bounded_and_terminal() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", None, Some(2), None).await.unwrap();
        for i in 0..2 {
            let owner = format!("turn-{i}");
            assert!(matches!(
                claim(&db, &wf.id, &owner, Some(60)).await.unwrap().unwrap(),
                Claim::Granted(_)
            ));
            release(&db, &wf.id, &owner, OUTCOME_CONTINUED, None)
                .await
                .unwrap();
        }
        // The budget is tripped eagerly by the final release, so the next claim
        // is refused on the persisted terminal state.
        let denied = claim(&db, &wf.id, "turn-3", Some(60))
            .await
            .unwrap()
            .unwrap();
        match denied {
            Claim::Denied(ClaimDenied::State(ref s)) => assert_eq!(s, STATE_EXHAUSTED),
            Claim::Denied(ClaimDenied::Exhausted { turns, max_turns }) => {
                assert_eq!((turns, max_turns), (2, 2))
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        let row = get(&db, &wf.id).await.unwrap().unwrap();
        assert_eq!(row.state, STATE_EXHAUSTED, "the stop is persisted");
        assert!(is_terminal(&row.state));
        // Still refused after a restart-equivalent re-read.
        assert!(matches!(
            claim(&db, &wf.id, "turn-4", Some(60))
                .await
                .unwrap()
                .unwrap(),
            Claim::Denied(ClaimDenied::State(_))
        ));
    }

    /// The counter is also checked defensively at claim time, so a workflow whose
    /// `turns` was advanced by another path (restart mid-release, manual fix)
    /// still refuses with an explicit budget reason.
    #[tokio::test]
    async fn claim_checks_the_turn_counter_defensively() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", None, Some(3), None).await.unwrap();
        sqlx::query("UPDATE workflows SET turns = 3 WHERE id = ?1")
            .bind(&wf.id)
            .execute(&db)
            .await
            .unwrap();
        match claim(&db, &wf.id, "turn-a", Some(60))
            .await
            .unwrap()
            .unwrap()
        {
            Claim::Denied(ClaimDenied::Exhausted { turns, max_turns }) => {
                assert_eq!((turns, max_turns), (3, 3))
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
        assert_eq!(
            get(&db, &wf.id).await.unwrap().unwrap().state,
            STATE_EXHAUSTED
        );
    }

    #[tokio::test]
    async fn wall_clock_budget_is_bounded() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", None, None, Some(60)).await.unwrap();
        sqlx::query("UPDATE workflows SET deadline_at = ?1 WHERE id = ?2")
            .bind(now_epoch() - 1)
            .bind(&wf.id)
            .execute(&db)
            .await
            .unwrap();
        match claim(&db, &wf.id, "turn-a", Some(60))
            .await
            .unwrap()
            .unwrap()
        {
            Claim::Denied(ClaimDenied::Expired { .. }) => {}
            other => panic!("expected Expired, got {other:?}"),
        }
        assert_eq!(
            get(&db, &wf.id).await.unwrap().unwrap().state,
            STATE_EXPIRED
        );
    }

    #[tokio::test]
    async fn waiting_for_human_blocks_autonomous_turns_but_records_the_reason() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", Some(ORIGIN), None, None)
            .await
            .unwrap();
        claim(&db, &wf.id, "turn-a", Some(60)).await.unwrap();
        let after = release(
            &db,
            &wf.id,
            "turn-a",
            OUTCOME_WAITING,
            Some("asked: deploy to prod or staging?"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(after.state, STATE_WAITING);
        assert_eq!(after.turns, 0, "a question does not consume turn budget");
        assert!(after.last_note.unwrap().contains("deploy to prod"));

        // No autonomous turn may proceed…
        match claim(&db, &wf.id, "turn-b", Some(60))
            .await
            .unwrap()
            .unwrap()
        {
            Claim::Denied(ClaimDenied::State(s)) => assert_eq!(s, STATE_WAITING),
            other => panic!("expected State(waiting), got {other:?}"),
        }
        // …but the human-reply path resumes it: resume() then claim succeeds.
        resume(&db, &wf.id, Some("user answered")).await.unwrap();
        assert!(matches!(
            claim(&db, &wf.id, "turn-b", Some(60))
                .await
                .unwrap()
                .unwrap(),
            Claim::Granted(_)
        ));
    }

    #[tokio::test]
    async fn lookup_by_origin_finds_the_task_for_a_chat_thread() {
        let (db, _d) = fresh_db().await;
        ensure(&db, "h", "sess-a", Some(ORIGIN), None, None)
            .await
            .unwrap();
        let other = r#"{"platform":"zulip","chat_id":"stream:eng","thread_id":"other-topic"}"#;
        ensure(&db, "h", "sess-b", Some(other), None, None)
            .await
            .unwrap();

        let f = Filter {
            origin_chat_id: Some("stream:eng".into()),
            origin_thread_id: Some("deploy-bug".into()),
            ..Default::default()
        };
        let found = list(&db, &f, 10).await.unwrap();
        assert_eq!(found.len(), 1, "exactly the workflow for that topic");
        assert_eq!(found[0].session_id, "sess-a");

        // Same stream, both topics.
        let f2 = Filter {
            origin_chat_id: Some("stream:eng".into()),
            ..Default::default()
        };
        assert_eq!(list(&db, &f2, 10).await.unwrap().len(), 2);
        // Unknown topic matches nothing rather than everything.
        let f3 = Filter {
            origin_thread_id: Some("nope".into()),
            ..Default::default()
        };
        assert!(list(&db, &f3, 10).await.unwrap().is_empty());
        // A workflow with no origin is never matched by an origin filter.
        ensure(&db, "h", "sess-c", None, None, None).await.unwrap();
        assert_eq!(list(&db, &f2, 10).await.unwrap().len(), 2);
        assert_eq!(list(&db, &Filter::default(), 10).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn release_rejects_unknown_outcomes_and_missing_workflows() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", None, None, None).await.unwrap();
        claim(&db, &wf.id, "o", Some(60)).await.unwrap();
        assert!(release(&db, &wf.id, "o", "sideways", None).await.is_err());
        assert!(release(&db, "wf-nope", "o", OUTCOME_DONE, None)
            .await
            .unwrap()
            .is_none());
        // noop leaves the state alone but frees the lease.
        let after = release(&db, &wf.id, "o", OUTCOME_NOOP, Some("nothing to do"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.state, STATE_ACTIVE);
        assert!(after.lease_owner.is_none());
    }

    #[tokio::test]
    async fn view_exposes_the_bounds_a_turn_needs() {
        let (db, _d) = fresh_db().await;
        let wf = ensure(&db, "h", "s", Some(ORIGIN), Some(3), Some(600))
            .await
            .unwrap();
        let v = view(&wf);
        assert_eq!(v["turns_remaining"], 3);
        assert_eq!(v["terminal"], false);
        assert_eq!(v["lease_held"], false);
        assert_eq!(v["origin"]["thread_id"], "deploy-bug");
        assert!(v["seconds_remaining"].as_i64().unwrap() > 500);
    }
}
