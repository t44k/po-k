//! Durable notification subscriptions (M15).
//!
//! po-k already pushes every session event and every derived-status change up
//! the WebSocket uplink; before M15 Xpo-k used those frames only to refresh its
//! routing map and the `xpok_sessions` row, then dropped them. An orchestrator
//! could therefore only learn that a CC turn finished by having an HTTP call
//! (`/wait`, `/events`) in flight at that exact moment — impossible for an
//! agent that has to handle other work in between.
//!
//! This module keeps the interest **on the server**: a subscription row says
//! "tell subscriber X about session S", and every matching frame is queued as a
//! notification row that survives an idle orchestrator, an Xpo-k restart, and a
//! po-k reconnect. Delivery is at-least-once, deduplicated on
//! `(sub_id, seq, kind)` for sequenced events.
//!
//! Cursor ownership: `subscriptions.cursor` is advanced by an **ack** only —
//! never by a read — so an unacked notification is always redelivered.

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::store::{now_epoch, now_iso, Db};

/// Event kinds a subscription watches when the caller doesn't name any: the
/// turn boundary, the two terminal lifecycle events, and the three "CC needs a
/// human" signals.
pub const DEFAULT_KINDS: &[&str] = &[
    "stop",
    "session_end",
    "cc_exited",
    "notification",
    "user_question",
    "permission_request",
];

/// Derived statuses that produce a notification when po-k reports a change.
/// These are the level-triggered safety net: even if the sequenced event that
/// caused the transition was never forwarded (old po-k, uplink down while it
/// happened), the status push still wakes the subscriber.
pub const DEFAULT_STATUSES: &[&str] = &["idle", "awaiting_input", "ended"];

/// Default subscription lifetime. Refreshed on every ack.
pub const DEFAULT_TTL_SECS: i64 = 24 * 3600;
pub const MAX_TTL_SECS: i64 = 7 * 24 * 3600;
/// Cap on how many persisted po-k events one reconnect replay pulls back.
pub const REPLAY_LIMIT: i64 = 200;

#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionRow {
    pub id: String,
    pub subscriber: String,
    pub sid: String,
    pub kinds: Vec<String>,
    pub statuses: Vec<String>,
    pub cursor: i64,
    pub created_at: String,
    /// Configured lifetime; reused when an ack refreshes `expires_at` so a
    /// short-lived subscription doesn't silently become a 24h one.
    pub ttl_secs: i64,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NotificationRow {
    pub id: String,
    pub subscription_id: String,
    pub subscriber: String,
    pub session_id: String,
    pub seq: i64,
    pub kind: String,
    pub status: Option<String>,
    pub payload: Value,
    pub created_at: String,
}

type SubTuple = (
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    i64,
    i64,
);

fn sub_from(t: SubTuple) -> SubscriptionRow {
    let (id, subscriber, sid, kinds, statuses, cursor, created_at, ttl_secs, expires_at) = t;
    SubscriptionRow {
        id,
        subscriber,
        sid,
        kinds: parse_list(&kinds),
        statuses: parse_list(&statuses),
        cursor,
        created_at,
        ttl_secs,
        expires_at,
    }
}

fn parse_list(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

const SUB_COLS: &str =
    "id, subscriber, sid, kinds, statuses, cursor, created_at, ttl_secs, expires_at";

// ---------------------------------------------------------------------------
// Long-poll hub
// ---------------------------------------------------------------------------

/// Per-subscriber wakeups for `GET /notifications?wait=`.
///
/// Waiters must arm (`Notified::enable()`) *before* querying the DB — the same
/// lost-wakeup trap po-k's `core::events::page` fell into: `notify_waiters()`
/// leaves no permit, so a notification enqueued between an unarmed query and
/// the park would sleep out the whole timeout.
#[derive(Clone, Default)]
pub struct NotifyHub {
    inner: Arc<DashMap<String, Arc<Notify>>>,
}

impl NotifyHub {
    pub fn waiter(&self, subscriber: &str) -> Arc<Notify> {
        self.inner
            .entry(subscriber.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub fn wake(&self, subscriber: &str) {
        if let Some(n) = self.inner.get(subscriber) {
            n.notify_waiters();
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription CRUD
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn create_subscription(
    db: &Db,
    subscriber: &str,
    sid: &str,
    kinds: &[String],
    statuses: &[String],
    cursor: i64,
    ttl_secs: i64,
) -> Result<SubscriptionRow> {
    let id = format!("sub-{}", Uuid::new_v4());
    let ttl = ttl_secs.clamp(1, MAX_TTL_SECS);
    let row = SubscriptionRow {
        id: id.clone(),
        subscriber: subscriber.to_string(),
        sid: sid.to_string(),
        kinds: kinds.to_vec(),
        statuses: statuses.to_vec(),
        cursor,
        created_at: now_iso(),
        ttl_secs: ttl,
        expires_at: now_epoch() + ttl,
    };
    sqlx::query(
        r#"INSERT INTO subscriptions (id, subscriber, sid, kinds, statuses, cursor, created_at, ttl_secs, expires_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"#,
    )
    .bind(&row.id)
    .bind(&row.subscriber)
    .bind(&row.sid)
    .bind(serde_json::to_string(&row.kinds)?)
    .bind(serde_json::to_string(&row.statuses)?)
    .bind(row.cursor)
    .bind(&row.created_at)
    .bind(row.ttl_secs)
    .bind(row.expires_at)
    .execute(db)
    .await
    .context("INSERT INTO subscriptions")?;
    Ok(row)
}

pub async fn get_subscription(db: &Db, id: &str) -> Result<Option<SubscriptionRow>> {
    let row: Option<SubTuple> = sqlx::query_as(&format!(
        "SELECT {SUB_COLS} FROM subscriptions WHERE id = ?1"
    ))
    .bind(id)
    .fetch_optional(db)
    .await
    .context("SELECT subscription")?;
    Ok(row.map(sub_from))
}

/// List subscriptions, optionally filtered by subscriber and/or session.
pub async fn list_subscriptions(
    db: &Db,
    subscriber: Option<&str>,
    sid: Option<&str>,
) -> Result<Vec<SubscriptionRow>> {
    let rows: Vec<SubTuple> = sqlx::query_as(&format!(
        r#"SELECT {SUB_COLS} FROM subscriptions
           WHERE (?1 IS NULL OR subscriber = ?1)
             AND (?2 IS NULL OR sid = ?2)
           ORDER BY created_at"#
    ))
    .bind(subscriber)
    .bind(sid)
    .fetch_all(db)
    .await
    .context("SELECT subscriptions")?;
    Ok(rows.into_iter().map(sub_from).collect())
}

/// Subscriptions for one session that haven't expired.
pub async fn active_for_session(db: &Db, sid: &str) -> Result<Vec<SubscriptionRow>> {
    let rows: Vec<SubTuple> = sqlx::query_as(&format!(
        "SELECT {SUB_COLS} FROM subscriptions WHERE sid = ?1 AND expires_at > ?2"
    ))
    .bind(sid)
    .bind(now_epoch())
    .fetch_all(db)
    .await
    .context("SELECT subscriptions for session")?;
    Ok(rows.into_iter().map(sub_from).collect())
}

/// Every unexpired subscription (used to drive reconnect replay).
pub async fn all_active(db: &Db) -> Result<Vec<SubscriptionRow>> {
    let rows: Vec<SubTuple> = sqlx::query_as(&format!(
        "SELECT {SUB_COLS} FROM subscriptions WHERE expires_at > ?1"
    ))
    .bind(now_epoch())
    .fetch_all(db)
    .await
    .context("SELECT active subscriptions")?;
    Ok(rows.into_iter().map(sub_from).collect())
}

/// Delete a subscription and every notification queued for it. Returns false
/// when the id is unknown (so the endpoint can 404).
pub async fn delete_subscription(db: &Db, id: &str) -> Result<bool> {
    let mut tx = db.begin().await?;
    sqlx::query("DELETE FROM notifications WHERE sub_id = ?1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("DELETE notifications for subscription")?;
    let res = sqlx::query("DELETE FROM subscriptions WHERE id = ?1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("DELETE subscription")?;
    tx.commit().await?;
    Ok(res.rows_affected() > 0)
}

/// Drop expired subscriptions (and their queued notifications) plus acked
/// notifications older than `ack_retention_secs`. Returns `(subs, notifs)`.
///
/// Acked rows are kept for a while on purpose: they are what makes a repeated
/// ack idempotent and a duplicate delivery detectable.
pub async fn sweep(db: &Db, ack_retention_secs: i64) -> Result<(u64, u64)> {
    let now = now_epoch();
    let expired: Vec<(String,)> =
        sqlx::query_as("SELECT id FROM subscriptions WHERE expires_at <= ?1")
            .bind(now)
            .fetch_all(db)
            .await
            .context("SELECT expired subscriptions")?;
    let mut notifs = 0;
    for (id,) in &expired {
        let r = sqlx::query("DELETE FROM notifications WHERE sub_id = ?1")
            .bind(id)
            .execute(db)
            .await
            .context("DELETE notifications of expired subscription")?;
        notifs += r.rows_affected();
    }
    let subs = sqlx::query("DELETE FROM subscriptions WHERE expires_at <= ?1")
        .bind(now)
        .execute(db)
        .await
        .context("DELETE expired subscriptions")?
        .rows_affected();
    // Prune long-acked rows. The cut-off MUST be rendered in the same format
    // `now_iso()` writes (`YYYY-MM-DDTHH:MM:SSZ`) — SQLite's bare `datetime()`
    // yields `YYYY-MM-DD HH:MM:SS`, and a lexicographic compare against the
    // 'T'/'Z' form then never matches within the same day, so nothing would
    // ever be pruned.
    let r = sqlx::query(
        "DELETE FROM notifications WHERE acked_at IS NOT NULL
           AND acked_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?1)",
    )
    .bind(format!("-{ack_retention_secs} seconds"))
    .execute(db)
    .await
    .context("DELETE old acked notifications")?;
    Ok((subs, notifs + r.rows_affected()))
}

// ---------------------------------------------------------------------------
// Matching + enqueue
// ---------------------------------------------------------------------------

/// Queue one notification.
///
/// Two deduplication lanes:
/// * `seq > 0` — sequenced events. The partial unique index on
///   `(sub_id, seq, kind)` makes a re-delivery (duplicate push, reconnect
///   replay) a no-op forever, which is exactly right for an immutable event.
/// * `seq <= 0` — status changes (`-1`) and events forwarded by a pre-M15 po-k
///   that carry no seq. These have no stable identity, so the unique index
///   would permanently swallow every later occurrence; instead they are
///   suppressed only while an unacked row with the same kind+status is
///   pending, and may fire again once that one is acked.
///
/// Returns the row when it was actually inserted, `None` when suppressed.
async fn enqueue(
    db: &Db,
    sub: &SubscriptionRow,
    seq: i64,
    kind: &str,
    status: Option<&str>,
    payload: &Value,
) -> Result<Option<NotificationRow>> {
    if seq <= 0 {
        let dup: Option<(String,)> = sqlx::query_as(
            r#"SELECT id FROM notifications
               WHERE sub_id = ?1 AND acked_at IS NULL AND kind = ?2
                 AND IFNULL(status,'') = IFNULL(?3,'')"#,
        )
        .bind(&sub.id)
        .bind(kind)
        .bind(status)
        .fetch_optional(db)
        .await
        .context("SELECT duplicate status notification")?;
        if dup.is_some() {
            return Ok(None);
        }
    }
    let row = NotificationRow {
        id: format!("ntf-{}", Uuid::new_v4()),
        subscription_id: sub.id.clone(),
        subscriber: sub.subscriber.clone(),
        session_id: sub.sid.clone(),
        seq,
        kind: kind.to_string(),
        status: status.map(String::from),
        payload: payload.clone(),
        created_at: now_iso(),
    };
    let res = sqlx::query(
        r#"INSERT OR IGNORE INTO notifications
             (id, sub_id, subscriber, sid, seq, kind, status, payload, created_at, acked_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL)"#,
    )
    .bind(&row.id)
    .bind(&row.subscription_id)
    .bind(&row.subscriber)
    .bind(&row.session_id)
    .bind(row.seq)
    .bind(&row.kind)
    .bind(&row.status)
    .bind(serde_json::to_string(&row.payload)?)
    .bind(&row.created_at)
    .execute(db)
    .await
    .context("INSERT INTO notifications")?;
    Ok((res.rows_affected() > 0).then_some(row))
}

/// Match a forwarded session event against every active subscription for that
/// session. Returns the subscribers that gained a notification (so the caller
/// can wake their long-polls).
///
/// A `seq` at or below a subscription's cursor is ignored: the subscriber has
/// already acked past it (this is what keeps reconnect replay quiet).
pub async fn match_event(
    db: &Db,
    sid: &str,
    kind: &str,
    seq: i64,
    ts: &str,
    payload: &Value,
) -> Result<Vec<String>> {
    let subs = active_for_session(db, sid).await?;
    let mut woken = Vec::new();
    for sub in subs {
        let kinds: &[String] = &sub.kinds;
        let watched = if kinds.is_empty() {
            DEFAULT_KINDS.contains(&kind)
        } else {
            kinds.iter().any(|k| k == kind)
        };
        if !watched {
            continue;
        }
        if seq > 0 && seq <= sub.cursor {
            continue;
        }
        let body = json!({ "event": { "kind": kind, "seq": seq, "ts": ts, "payload": payload } });
        // An event with no usable seq (pre-M15 po-k) is stored on the
        // unsequenced lane so it can recur after an ack; the real value stays
        // visible in the payload.
        let stored_seq = if seq > 0 { seq } else { -1 };
        if enqueue(db, &sub, stored_seq, kind, None, &body)
            .await?
            .is_some()
        {
            woken.push(sub.subscriber.clone());
        }
    }
    Ok(woken)
}

/// Match a derived-status change (the level-triggered path).
pub async fn match_status(db: &Db, sid: &str, status: &str) -> Result<Vec<String>> {
    let subs = active_for_session(db, sid).await?;
    let mut woken = Vec::new();
    for sub in subs {
        let statuses: &[String] = &sub.statuses;
        let watched = if statuses.is_empty() {
            DEFAULT_STATUSES.contains(&status)
        } else {
            statuses.iter().any(|s| s == status)
        };
        if !watched {
            continue;
        }
        let body = json!({ "status": status });
        if enqueue(db, &sub, -1, "status", Some(status), &body)
            .await?
            .is_some()
        {
            woken.push(sub.subscriber.clone());
        }
    }
    Ok(woken)
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

type NotifTuple = (
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    String,
);

fn notif_from(t: NotifTuple) -> NotificationRow {
    let (id, sub_id, subscriber, sid, seq, kind, status, payload, created_at) = t;
    NotificationRow {
        id,
        subscription_id: sub_id,
        subscriber,
        session_id: sid,
        seq,
        kind,
        status,
        payload: payload
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or(Value::Null),
        created_at,
    }
}

const NOTIF_COLS: &str = "id, sub_id, subscriber, sid, seq, kind, status, payload, created_at";

/// Unacked notifications, oldest first.
pub async fn pending(
    db: &Db,
    subscriber: Option<&str>,
    sid: Option<&str>,
    limit: i64,
) -> Result<Vec<NotificationRow>> {
    let rows: Vec<NotifTuple> = sqlx::query_as(&format!(
        r#"SELECT {NOTIF_COLS} FROM notifications
           WHERE acked_at IS NULL
             AND (?1 IS NULL OR subscriber = ?1)
             AND (?2 IS NULL OR sid = ?2)
           ORDER BY created_at, rowid LIMIT ?3"#
    ))
    .bind(subscriber)
    .bind(sid)
    .bind(limit.clamp(1, 500))
    .fetch_all(db)
    .await
    .context("SELECT pending notifications")?;
    Ok(rows.into_iter().map(notif_from).collect())
}

/// Ack notifications by id. Idempotent: an id that is unknown or already acked
/// contributes to `already` instead of `acked`, and never errors.
///
/// Acking advances the owning subscription's cursor to the highest acked seq
/// and refreshes its TTL — the subscription stays alive as long as someone is
/// actually consuming it.
pub async fn ack(db: &Db, ids: &[String]) -> Result<(u64, u64)> {
    let mut acked = 0;
    let mut already = 0;
    for id in ids {
        let row: Option<(String, i64)> = sqlx::query_as(
            "SELECT sub_id, seq FROM notifications WHERE id = ?1 AND acked_at IS NULL",
        )
        .bind(id)
        .fetch_optional(db)
        .await
        .context("SELECT notification to ack")?;
        let Some((sub_id, seq)) = row else {
            already += 1;
            continue;
        };
        // Refresh with the subscription's CONFIGURED ttl, not the default —
        // acking a 60s subscription must not silently make it a 24h one.
        let ttl: i64 =
            sqlx::query_as::<_, (i64,)>("SELECT ttl_secs FROM subscriptions WHERE id = ?1")
                .bind(&sub_id)
                .fetch_optional(db)
                .await
                .context("SELECT subscription ttl")?
                .map(|(t,)| t)
                .unwrap_or(DEFAULT_TTL_SECS);
        let mut tx = db.begin().await?;
        sqlx::query("UPDATE notifications SET acked_at = ?1 WHERE id = ?2 AND acked_at IS NULL")
            .bind(now_iso())
            .bind(id)
            .execute(&mut *tx)
            .await
            .context("UPDATE notification acked_at")?;
        sqlx::query(
            "UPDATE subscriptions SET cursor = MAX(cursor, ?1), expires_at = ?2 WHERE id = ?3",
        )
        .bind(seq.max(0))
        .bind(now_epoch() + ttl)
        .bind(&sub_id)
        .execute(&mut *tx)
        .await
        .context("UPDATE subscription cursor")?;
        tx.commit().await?;
        acked += 1;
    }
    Ok((acked, already))
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

    async fn sub_for(db: &Db, sid: &str, cursor: i64) -> SubscriptionRow {
        create_subscription(db, "hermes-1", sid, &[], &[], cursor, DEFAULT_TTL_SECS)
            .await
            .unwrap()
    }

    fn ev() -> Value {
        json!({"text": "done"})
    }

    #[tokio::test]
    async fn matching_event_enqueues_one_notification() {
        let (db, _d) = fresh_db().await;
        let sub = sub_for(&db, "s1", 0).await;
        let woken = match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        assert_eq!(woken, vec!["hermes-1".to_string()]);

        let pend = pending(&db, Some("hermes-1"), None, 10).await.unwrap();
        assert_eq!(pend.len(), 1);
        assert_eq!(pend[0].kind, "stop");
        assert_eq!(pend[0].seq, 5);
        assert_eq!(pend[0].session_id, "s1");
        assert_eq!(pend[0].subscription_id, sub.id);
    }

    #[tokio::test]
    async fn no_subscription_is_a_no_op() {
        let (db, _d) = fresh_db().await;
        assert!(match_event(&db, "ghost", "stop", 5, "t", &ev())
            .await
            .unwrap()
            .is_empty());
        assert!(match_status(&db, "ghost", "idle").await.unwrap().is_empty());
        assert!(pending(&db, None, None, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unwatched_kinds_and_statuses_are_ignored() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        // tool_use is not in DEFAULT_KINDS; working is not in DEFAULT_STATUSES.
        assert!(match_event(&db, "s1", "tool_use", 5, "t", &ev())
            .await
            .unwrap()
            .is_empty());
        assert!(match_status(&db, "s1", "working").await.unwrap().is_empty());
        assert!(pending(&db, None, None, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn explicit_kind_list_overrides_defaults() {
        let (db, _d) = fresh_db().await;
        create_subscription(
            &db,
            "hermes-1",
            "s1",
            &["tool_use".into()],
            &[],
            0,
            DEFAULT_TTL_SECS,
        )
        .await
        .unwrap();
        assert_eq!(
            match_event(&db, "s1", "tool_use", 1, "t", &ev())
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(match_event(&db, "s1", "stop", 2, "t", &ev())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn duplicate_sequenced_event_is_suppressed() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        // Same seq again (duplicate push, or a reconnect replay window overlap).
        let second = match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        assert!(second.is_empty(), "no second wake");
        assert_eq!(pending(&db, None, None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn events_at_or_below_cursor_are_skipped() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 10).await; // subscribed at "now" = seq 10
        assert!(match_event(&db, "s1", "stop", 7, "t", &ev())
            .await
            .unwrap()
            .is_empty());
        assert!(match_event(&db, "s1", "stop", 10, "t", &ev())
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            match_event(&db, "s1", "stop", 11, "t", &ev())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn status_notification_dedupes_while_unacked_then_reopens() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        assert_eq!(match_status(&db, "s1", "idle").await.unwrap().len(), 1);
        // Second idle push while the first is unacked → suppressed.
        assert!(match_status(&db, "s1", "idle").await.unwrap().is_empty());
        // A different status is its own notification.
        assert_eq!(
            match_status(&db, "s1", "awaiting_input")
                .await
                .unwrap()
                .len(),
            1
        );
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend.len(), 2);

        // Once acked, a later transition to the same status notifies again.
        let ids: Vec<String> = pend.iter().map(|n| n.id.clone()).collect();
        ack(&db, &ids).await.unwrap();
        assert_eq!(match_status(&db, "s1", "idle").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ack_is_idempotent_and_advances_the_cursor() {
        let (db, _d) = fresh_db().await;
        let sub = sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 9, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();

        let (acked, already) = ack(&db, std::slice::from_ref(&id)).await.unwrap();
        assert_eq!((acked, already), (1, 0));
        assert!(pending(&db, None, None, 10).await.unwrap().is_empty());

        // Re-ack (retry / duplicate) and an unknown id: counted, never an error.
        let (acked2, already2) = ack(&db, &[id, "ntf-nope".into()]).await.unwrap();
        assert_eq!((acked2, already2), (0, 2));

        let after = get_subscription(&db, &sub.id).await.unwrap().unwrap();
        assert_eq!(after.cursor, 9, "cursor advanced to the acked seq");
        assert!(
            after.expires_at > sub.expires_at - 1,
            "TTL refreshed on ack"
        );

        // And the acked seq is now below the cursor, so a replay is quiet.
        assert!(match_event(&db, "s1", "stop", 9, "t", &ev())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn reading_does_not_consume_so_delivery_is_at_least_once() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 3, "t", &ev()).await.unwrap();
        // Poll repeatedly without acking (simulating a consumer that crashed
        // before acting): the notification must still be there.
        for _ in 0..3 {
            assert_eq!(
                pending(&db, Some("hermes-1"), None, 10)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn unacked_notifications_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.db");
        {
            let db = store::open(&path).await.unwrap();
            sub_for(&db, "s1", 0).await;
            match_event(&db, "s1", "stop", 4, "t", &ev()).await.unwrap();
            db.close().await;
        }
        // Fresh pool over the same file = Xpo-k restarted.
        let db2 = store::open(&path).await.unwrap();
        let pend = pending(&db2, Some("hermes-1"), None, 10).await.unwrap();
        assert_eq!(pend.len(), 1);
        assert_eq!(pend[0].seq, 4);
        assert_eq!(
            all_active(&db2).await.unwrap().len(),
            1,
            "subscription persisted too"
        );
    }

    #[tokio::test]
    async fn list_and_delete_subscriptions() {
        let (db, _d) = fresh_db().await;
        let a = sub_for(&db, "s1", 0).await;
        create_subscription(&db, "other", "s2", &[], &[], 0, DEFAULT_TTL_SECS)
            .await
            .unwrap();
        assert_eq!(list_subscriptions(&db, None, None).await.unwrap().len(), 2);
        assert_eq!(
            list_subscriptions(&db, Some("hermes-1"), None)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            list_subscriptions(&db, None, Some("s2"))
                .await
                .unwrap()
                .len(),
            1
        );

        match_event(&db, "s1", "stop", 1, "t", &ev()).await.unwrap();
        assert!(delete_subscription(&db, &a.id).await.unwrap());
        assert!(
            !delete_subscription(&db, &a.id).await.unwrap(),
            "second delete is false"
        );
        assert!(
            pending(&db, None, None, 10).await.unwrap().is_empty(),
            "queued rows go with the subscription"
        );
    }

    #[tokio::test]
    async fn sweep_removes_expired_subscriptions_and_their_notifications() {
        let (db, _d) = fresh_db().await;
        // Expired 60s ago (deterministic: the TTL is written, not slept through).
        let expired = create_subscription(&db, "hermes-1", "s1", &[], &[], 0, 1)
            .await
            .unwrap();
        sqlx::query("UPDATE subscriptions SET expires_at = ?1 WHERE id = ?2")
            .bind(store::now_epoch() - 60)
            .bind(&expired.id)
            .execute(&db)
            .await
            .unwrap();
        match_event(&db, "s1", "stop", 1, "t", &ev()).await.unwrap();
        // An expired subscription no longer matches new events…
        assert!(active_for_session(&db, "s1").await.unwrap().is_empty());
        let live = sub_for(&db, "s2", 0).await;
        match_event(&db, "s2", "stop", 1, "t", &ev()).await.unwrap();

        let (subs_gone, _) = sweep(&db, 3600).await.unwrap();
        assert_eq!(subs_gone, 1);
        assert!(get_subscription(&db, &expired.id).await.unwrap().is_none());
        assert!(get_subscription(&db, &live.id).await.unwrap().is_some());
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend.len(), 1, "only the live subscription's row remains");
        assert_eq!(pend[0].session_id, "s2");
    }

    #[tokio::test]
    async fn unsequenced_events_from_an_old_pok_still_notify() {
        // An old po-k forwards no seq (0). It can't be deduped or resumed, but
        // it must still reach the subscriber.
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 42).await;
        assert_eq!(
            match_event(&db, "s1", "stop", 0, "", &ev())
                .await
                .unwrap()
                .len(),
            1
        );
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend[0].seq, -1, "stored on the unsequenced lane");
        assert_eq!(
            pend[0].payload["event"]["seq"], 0,
            "the forwarded value stays visible in the payload"
        );
    }

    /// Regression: an unsequenced event must not be locked out forever by the
    /// `(sub_id, seq, kind)` unique index. A second turn from a pre-M15 po-k has
    /// to notify again once the first notification is acked — otherwise the
    /// orchestrator would see exactly one completion per session, ever.
    #[tokio::test]
    async fn unsequenced_events_recur_after_ack() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        assert_eq!(
            match_event(&db, "s1", "stop", 0, "", &ev())
                .await
                .unwrap()
                .len(),
            1
        );
        // While the first is unacked, a repeat is suppressed (no nagging).
        assert!(match_event(&db, "s1", "stop", 0, "", &ev())
            .await
            .unwrap()
            .is_empty());
        let ids: Vec<String> = pending(&db, None, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        assert_eq!(ids.len(), 1);
        ack(&db, &ids).await.unwrap();
        // The next turn's stop notifies again.
        assert_eq!(
            match_event(&db, "s1", "stop", 0, "", &ev())
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(pending(&db, None, None, 10).await.unwrap().len(), 1);
    }

    /// Regression: acking must refresh the subscription with its OWN ttl, not
    /// the 24h default — a deliberately short-lived subscription must stay
    /// short-lived.
    #[tokio::test]
    async fn ack_refreshes_with_the_configured_ttl() {
        let (db, _d) = fresh_db().await;
        let short = create_subscription(&db, "hermes-1", "s1", &[], &[], 0, 120)
            .await
            .unwrap();
        assert_eq!(short.ttl_secs, 120);
        match_event(&db, "s1", "stop", 4, "t", &ev()).await.unwrap();
        let ids: Vec<String> = pending(&db, None, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        ack(&db, &ids).await.unwrap();
        let after = get_subscription(&db, &short.id).await.unwrap().unwrap();
        let horizon = after.expires_at - now_epoch();
        assert!(
            (60..=180).contains(&horizon),
            "expected a ~120s horizon, got {horizon}s (did the default TTL leak in?)"
        );
    }

    /// Regression: acked rows must actually be pruned. The cut-off has to be
    /// rendered in `now_iso()`'s format — with SQLite's bare `datetime()` the
    /// comparison silently never matched and acked rows accumulated forever.
    #[tokio::test]
    async fn sweep_prunes_acked_notifications_past_the_retention_window() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 1, "t", &ev()).await.unwrap();
        let ids: Vec<String> = pending(&db, None, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        ack(&db, &ids).await.unwrap();

        // Age the ack deterministically instead of sleeping.
        sqlx::query(
            "UPDATE notifications SET acked_at = strftime('%Y-%m-%dT%H:%M:%SZ','now','-7200 seconds')",
        )
        .execute(&db)
        .await
        .unwrap();
        let (_, notifs) = sweep(&db, 3600).await.unwrap();
        assert_eq!(notifs, 1, "the 2h-old acked row must be pruned");
        let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(total.0, 0);
    }

    /// …but a *recent* ack is retained, because that retention window is what
    /// makes a duplicate ack idempotent instead of "unknown id".
    #[tokio::test]
    async fn sweep_keeps_recently_acked_rows_for_idempotence() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 1, "t", &ev()).await.unwrap();
        let ids: Vec<String> = pending(&db, None, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        ack(&db, &ids).await.unwrap();
        let (_, notifs) = sweep(&db, 3600).await.unwrap();
        assert_eq!(notifs, 0, "a fresh ack is inside the retention window");
        let (acked, already) = ack(&db, &ids).await.unwrap();
        assert_eq!(
            (acked, already),
            (0, 1),
            "re-ack still reports already_acked"
        );
    }

    #[tokio::test]
    async fn create_clamps_the_ttl_and_persists_it() {
        let (db, _d) = fresh_db().await;
        let huge = create_subscription(&db, "h", "s1", &[], &[], 0, MAX_TTL_SECS * 10)
            .await
            .unwrap();
        assert_eq!(huge.ttl_secs, MAX_TTL_SECS);
        let zero = create_subscription(&db, "h", "s2", &[], &[], 0, 0)
            .await
            .unwrap();
        assert_eq!(
            zero.ttl_secs, 1,
            "a non-positive ttl clamps to 1s, never 0 or negative"
        );
    }
}
