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

/// Maximum serialised size of the opaque `origin` blob. Big enough for chat
/// routing metadata, small enough that it can never become a payload channel.
pub const ORIGIN_MAX_BYTES: usize = 2048;
/// Longest accepted value for a single origin field.
pub const ORIGIN_MAX_FIELD: usize = 300;
/// The only keys an orchestrator may store as origin. Everything else is
/// dropped: this is routing metadata, never a place for prose or secrets.
pub const ORIGIN_KEYS: &[&str] = &[
    "platform",
    "chat_id",
    "chat_name",
    "chat_type",
    "thread_id",
    "parent_chat_id",
    "message_id",
    "user_id",
    "user_name",
    "session_key",
    "hint",
];

/// Validate and normalise an `origin` object.
///
/// Returns the canonical JSON string to persist, or `None` when nothing usable
/// was supplied. Rejects (rather than truncates) anything structurally wrong so
/// a caller learns immediately; drops unknown keys silently so the orchestrator
/// can evolve without a lockstep Xpo-k upgrade.
pub fn sanitize_origin(raw: &Value) -> Result<Option<String>, String> {
    match raw {
        Value::Null => return Ok(None),
        Value::Object(_) => {}
        other => {
            return Err(format!(
                "origin must be an object, got {}",
                match other {
                    Value::Array(_) => "array",
                    Value::String(_) => "string",
                    Value::Number(_) => "number",
                    Value::Bool(_) => "boolean",
                    _ => "value",
                }
            ))
        }
    }
    let obj = raw.as_object().expect("checked above");
    let mut out = serde_json::Map::new();
    for key in ORIGIN_KEYS {
        let Some(v) = obj.get(*key) else { continue };
        let text = match v {
            Value::Null => continue,
            Value::String(s) => s.trim().to_string(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => return Err(format!("origin.{key} must be a scalar")),
        };
        if text.is_empty() {
            continue;
        }
        if text.chars().count() > ORIGIN_MAX_FIELD {
            return Err(format!(
                "origin.{key} is longer than {ORIGIN_MAX_FIELD} characters"
            ));
        }
        // Control characters would corrupt log lines and prompt templates.
        if text.chars().any(|c| c.is_control()) {
            return Err(format!("origin.{key} must not contain control characters"));
        }
        out.insert((*key).to_string(), Value::String(text));
    }
    if out.is_empty() {
        return Ok(None);
    }
    let encoded = serde_json::to_string(&Value::Object(out)).map_err(|e| e.to_string())?;
    if encoded.len() > ORIGIN_MAX_BYTES {
        return Err(format!("origin exceeds {ORIGIN_MAX_BYTES} bytes"));
    }
    Ok(Some(encoded))
}

/// Parse a stored origin blob back into JSON, defaulting to `{}`.
pub fn origin_value(raw: Option<&str>) -> Value {
    raw.and_then(|o| serde_json::from_str(o).ok())
        .unwrap_or_else(|| json!({}))
}

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
    /// Webhook target for push delivery (M16). `None` = poll-only subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_url: Option<String>,
    /// Name of the env var holding the HMAC secret. The secret itself is never
    /// persisted, logged, or returned by the API.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_secret_env: Option<String>,
    /// Path to a file holding the HMAC secret (alternative to the env var).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_secret_file: Option<String>,
    /// Opaque, validated routing metadata: which chat/topic/user this work came
    /// from (M17). Stored and echoed verbatim, never interpreted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// The workflow this subscription belongs to (M17).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
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
    /// none | pending | delivered | failed (M16). Independent of `acked_at`.
    pub delivery_state: String,
    pub delivery_attempts: i64,
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
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn sub_from(t: SubTuple) -> SubscriptionRow {
    let (
        id,
        subscriber,
        sid,
        kinds,
        statuses,
        cursor,
        created_at,
        ttl_secs,
        expires_at,
        deliver_url,
        deliver_secret_env,
        deliver_secret_file,
        origin,
        workflow_id,
    ) = t;
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
        deliver_url,
        deliver_secret_env,
        deliver_secret_file,
        origin,
        workflow_id,
    }
}

fn parse_list(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

const SUB_COLS: &str = "id, subscriber, sid, kinds, statuses, cursor, created_at, ttl_secs, \
     expires_at, deliver_url, deliver_secret_env, deliver_secret_file, origin, workflow_id";

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

/// Where and how to push notifications for a subscription. The secret is
/// referenced by env-var name or file path — never by value — so it cannot end
/// up in the database, a log line, or an API response.
#[derive(Debug, Clone, Default)]
pub struct DeliverySpec {
    pub url: Option<String>,
    pub secret_env: Option<String>,
    pub secret_file: Option<String>,
}

impl DeliverySpec {
    pub fn is_configured(&self) -> bool {
        self.url.as_deref().is_some_and(|u| !u.is_empty())
    }

    /// Which source the secret comes from, for display only.
    pub fn secret_source(&self) -> &'static str {
        if self.secret_env.is_some() {
            "env"
        } else if self.secret_file.is_some() {
            "file"
        } else {
            "none"
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn create_subscription(
    db: &Db,
    subscriber: &str,
    sid: &str,
    kinds: &[String],
    statuses: &[String],
    cursor: i64,
    ttl_secs: i64,
    deliver: &DeliverySpec,
    origin: Option<&str>,
    workflow_id: Option<&str>,
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
        deliver_url: deliver.url.clone(),
        deliver_secret_env: deliver.secret_env.clone(),
        deliver_secret_file: deliver.secret_file.clone(),
        origin: origin.map(String::from),
        workflow_id: workflow_id.map(String::from),
    };
    sqlx::query(
        r#"INSERT INTO subscriptions
             (id, subscriber, sid, kinds, statuses, cursor, created_at, ttl_secs, expires_at,
              deliver_url, deliver_secret_env, deliver_secret_file, origin, workflow_id)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)"#,
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
    .bind(&row.deliver_url)
    .bind(&row.deliver_secret_env)
    .bind(&row.deliver_secret_file)
    .bind(&row.origin)
    .bind(&row.workflow_id)
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

/// Point a subscription at a webhook target (or clear it). Returns the updated
/// row, or `None` when the id is unknown.
///
/// Clearing leaves already-queued notifications alone: they stay unacked and
/// pollable, they just stop being pushed.
pub async fn set_delivery(
    db: &Db,
    id: &str,
    spec: &DeliverySpec,
) -> Result<Option<SubscriptionRow>> {
    let res = sqlx::query(
        r#"UPDATE subscriptions
           SET deliver_url = ?1, deliver_secret_env = ?2, deliver_secret_file = ?3
           WHERE id = ?4"#,
    )
    .bind(&spec.url)
    .bind(&spec.secret_env)
    .bind(&spec.secret_file)
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE subscription delivery target")?;
    if res.rows_affected() == 0 {
        return Ok(None);
    }
    // Arm any already-queued rows for the new target so a subscription that was
    // poll-only starts pushing what it already holds.
    if spec.is_configured() {
        sqlx::query(
            r#"UPDATE notifications
               SET delivery_state = 'pending', next_attempt_at = ?1
               WHERE sub_id = ?2 AND acked_at IS NULL AND delivery_state IN ('none', 'failed')"#,
        )
        .bind(now_epoch())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE notifications for new delivery target")?;
    }
    get_subscription(db, id).await
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
    // A subscription with a webhook target starts its notification in
    // `pending` so the delivery loop picks it up on its next pass; otherwise
    // the row is poll-only and never enters the delivery state machine.
    let (delivery_state, next_attempt_at) = if sub.deliver_url.is_some() {
        ("pending", Some(now_epoch()))
    } else {
        ("none", None)
    };
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
        delivery_state: delivery_state.to_string(),
        delivery_attempts: 0,
    };
    let res = sqlx::query(
        r#"INSERT OR IGNORE INTO notifications
             (id, sub_id, subscriber, sid, seq, kind, status, payload, created_at, acked_at,
              delivery_state, delivery_attempts, next_attempt_at)
           VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,?10,0,?11)"#,
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
    .bind(&row.delivery_state)
    .bind(next_attempt_at)
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
    String,
    i64,
);

fn notif_from(t: NotifTuple) -> NotificationRow {
    let (
        id,
        sub_id,
        subscriber,
        sid,
        seq,
        kind,
        status,
        payload,
        created_at,
        delivery_state,
        delivery_attempts,
    ) = t;
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
        delivery_state,
        delivery_attempts,
    }
}

const NOTIF_COLS: &str = "id, sub_id, subscriber, sid, seq, kind, status, payload, created_at, \
     delivery_state, delivery_attempts";

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

// ---------------------------------------------------------------------------
// Webhook delivery state (M16)
// ---------------------------------------------------------------------------

/// A `notifications` row joined with its subscription's delivery target.
/// `NotifTuple` fields first (so `notif_from` can consume them), then
/// `deliver_url`, `deliver_secret_env`, `deliver_secret_file`.
type DueTuple = (
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// One notification that is due for a webhook push, with everything the
/// delivery worker needs — including the *reference* to the secret, never the
/// secret itself.
#[derive(Debug, Clone)]
pub struct DueDelivery {
    pub notification: NotificationRow,
    pub url: String,
    pub secret_env: Option<String>,
    pub secret_file: Option<String>,
    /// Correlation context from the owning subscription (M17), echoed in the
    /// push envelope so the woken turn knows which task and which chat thread.
    pub workflow_id: Option<String>,
    pub origin: Value,
}

/// Notifications whose push is due now.
///
/// Requirements, all enforced in SQL so a restart resumes correctly:
/// * the notification is still **unacked** (a handled one needs no push),
/// * its delivery is `pending` and `next_attempt_at` has passed,
/// * the owning subscription still exists, has a webhook target, and has not
///   expired.
pub async fn due_deliveries(db: &Db, now: i64, limit: i64) -> Result<Vec<DueDelivery>> {
    let rows: Vec<DueTuple> = sqlx::query_as(
        r#"SELECT n.id, n.sub_id, n.subscriber, n.sid, n.seq, n.kind, n.status, n.payload,
                  n.created_at, n.delivery_state, n.delivery_attempts,
                  s.deliver_url, s.deliver_secret_env, s.deliver_secret_file,
                  s.workflow_id, s.origin
           FROM notifications n
           JOIN subscriptions s ON s.id = n.sub_id
           WHERE n.acked_at IS NULL
             AND n.delivery_state = 'pending'
             AND IFNULL(n.next_attempt_at, 0) <= ?1
             AND s.deliver_url IS NOT NULL
             AND s.expires_at > ?1
           ORDER BY IFNULL(n.next_attempt_at, 0), n.rowid
           LIMIT ?2"#,
    )
    .bind(now)
    .bind(limit.clamp(1, 200))
    .fetch_all(db)
    .await
    .context("SELECT due deliveries")?;

    Ok(rows
        .into_iter()
        .map(|r| DueDelivery {
            notification: notif_from((r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9, r.10)),
            url: r.11,
            secret_env: r.12,
            secret_file: r.13,
            workflow_id: r.14,
            origin: origin_value(r.15.as_deref()),
        })
        .collect())
}

/// Mark a push as delivered. Deliberately does NOT touch `acked_at`: delivery
/// means "Hermes was told", acking means "Hermes handled it".
pub async fn mark_delivered(db: &Db, id: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE notifications
           SET delivery_state = 'delivered', delivered_at = ?1,
               delivery_attempts = delivery_attempts + 1,
               next_attempt_at = NULL, last_delivery_error = NULL
           WHERE id = ?2"#,
    )
    .bind(now_iso())
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE notification delivered")?;
    Ok(())
}

/// Record a failed attempt. `retry_at = None` parks the row as `failed` (either
/// a permanent rejection or the attempt budget is spent) — the notification
/// stays **unacked and pollable**, so the cron fallback still delivers it.
pub async fn record_failure(db: &Db, id: &str, error: &str, retry_at: Option<i64>) -> Result<()> {
    let state = if retry_at.is_some() {
        "pending"
    } else {
        "failed"
    };
    // Truncate: the error text is operator-facing, not a log sink.
    let err: String = error.chars().take(300).collect();
    sqlx::query(
        r#"UPDATE notifications
           SET delivery_state = ?1, delivery_attempts = delivery_attempts + 1,
               next_attempt_at = ?2, last_delivery_error = ?3
           WHERE id = ?4"#,
    )
    .bind(state)
    .bind(retry_at)
    .bind(err)
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE notification delivery failure")?;
    Ok(())
}

/// Operator view of a notification's delivery state (for `GET /subscriptions`).
pub async fn delivery_summary(db: &Db, sub_id: &str) -> Result<Value> {
    let row: Option<(i64, i64, i64, i64)> = sqlx::query_as(
        r#"SELECT
             SUM(CASE WHEN delivery_state = 'pending'   THEN 1 ELSE 0 END),
             SUM(CASE WHEN delivery_state = 'delivered' THEN 1 ELSE 0 END),
             SUM(CASE WHEN delivery_state = 'failed'    THEN 1 ELSE 0 END),
             SUM(CASE WHEN acked_at IS NULL             THEN 1 ELSE 0 END)
           FROM notifications WHERE sub_id = ?1"#,
    )
    .bind(sub_id)
    .fetch_optional(db)
    .await
    .context("SELECT delivery summary")?;
    let (p, d, f, u) = row.unwrap_or((0, 0, 0, 0));
    Ok(json!({
        "delivery_pending": p,
        "delivered": d,
        "delivery_failed": f,
        "unacked": u,
    }))
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
        create_subscription(
            db,
            "hermes-1",
            sid,
            &[],
            &[],
            cursor,
            DEFAULT_TTL_SECS,
            &DeliverySpec::default(),
            None,
            None,
        )
        .await
        .unwrap()
    }

    /// A subscription with a webhook target (secret referenced, never stored).
    async fn push_sub(db: &Db, sid: &str) -> SubscriptionRow {
        create_subscription(
            db,
            "hermes-1",
            sid,
            &[],
            &[],
            0,
            DEFAULT_TTL_SECS,
            &DeliverySpec {
                url: Some("http://127.0.0.1:9/webhooks/pok".into()),
                secret_env: Some("POK_WEBHOOK_SECRET".into()),
                secret_file: None,
            },
            None,
            None,
        )
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
            &DeliverySpec::default(),
            None,
            None,
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
        create_subscription(
            &db,
            "other",
            "s2",
            &[],
            &[],
            0,
            DEFAULT_TTL_SECS,
            &DeliverySpec::default(),
            None,
            None,
        )
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
        let expired = create_subscription(
            &db,
            "hermes-1",
            "s1",
            &[],
            &[],
            0,
            1,
            &DeliverySpec::default(),
            None,
            None,
        )
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
        let short = create_subscription(
            &db,
            "hermes-1",
            "s1",
            &[],
            &[],
            0,
            120,
            &DeliverySpec::default(),
            None,
            None,
        )
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

    // --- M17: origin validation ---

    #[test]
    fn sanitize_origin_keeps_allow_listed_routing_fields() {
        let raw = json!({
            "platform": "zulip",
            "chat_id": "stream:eng",
            "chat_name": "eng",
            "chat_type": "channel",
            "thread_id": "deploy-bug",
            "parent_chat_id": "stream:eng",
            "message_id": "12345",
            "user_id": "42",
            "user_name": "Tamas",
            "session_key": "agent:main:zulip:channel:stream:eng:deploy-bug",
            "hint": "asked in #eng > deploy-bug",
        });
        let encoded = sanitize_origin(&raw).unwrap().unwrap();
        let out: Value = serde_json::from_str(&encoded).unwrap();
        for key in ORIGIN_KEYS {
            assert!(out.get(*key).is_some(), "{key} should survive");
        }
    }

    #[test]
    fn sanitize_origin_drops_unknown_keys_and_never_carries_prose_or_secrets() {
        let raw = json!({
            "chat_id": "stream:eng",
            "token": "super-secret",
            "authorization": "Bearer abc",
            "last_assistant_message": "a long CC answer…",
            "transcript": ["turn one", "turn two"],
        });
        let out: Value = serde_json::from_str(&sanitize_origin(&raw).unwrap().unwrap()).unwrap();
        assert_eq!(
            out.as_object().unwrap().len(),
            1,
            "only chat_id survives: {out}"
        );
        assert_eq!(out["chat_id"], "stream:eng");
        let encoded = serde_json::to_string(&out).unwrap();
        assert!(!encoded.contains("super-secret"));
        assert!(!encoded.contains("Bearer"));
        assert!(!encoded.contains("CC answer"));
    }

    #[test]
    fn sanitize_origin_coerces_scalars_and_skips_blanks() {
        let out: Value = serde_json::from_str(
            &sanitize_origin(&json!({
                "chat_id": "  stream:eng  ",
                "message_id": 12345,
                "user_id": "",
                "thread_id": Value::Null,
            }))
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(out["chat_id"], "stream:eng", "trimmed");
        assert_eq!(out["message_id"], "12345", "numbers become strings");
        assert!(out.get("user_id").is_none(), "blank dropped");
        assert!(out.get("thread_id").is_none(), "null dropped");
    }

    #[test]
    fn sanitize_origin_rejects_bad_shapes_and_oversize_values() {
        assert!(sanitize_origin(&Value::Null).unwrap().is_none());
        assert!(sanitize_origin(&json!({})).unwrap().is_none());
        assert!(sanitize_origin(&json!({"unknown": "x"})).unwrap().is_none());
        for bad in [json!("string"), json!([1, 2]), json!(7), json!(true)] {
            assert!(sanitize_origin(&bad).is_err(), "{bad} must be rejected");
        }
        // Nested values are not scalars.
        assert!(sanitize_origin(&json!({"chat_id": {"a": 1}})).is_err());
        assert!(sanitize_origin(&json!({"chat_id": ["a"]})).is_err());
        // Field length cap.
        let long = "x".repeat(ORIGIN_MAX_FIELD + 1);
        let e = sanitize_origin(&json!({"hint": long})).unwrap_err();
        assert!(e.contains("longer than"), "{e}");
        // Control characters would corrupt log lines and prompt templates.
        assert!(sanitize_origin(&json!({"chat_id": "a\nb"})).is_err());
        // Whole-blob cap: many max-length fields.
        let big: serde_json::Map<String, Value> = ORIGIN_KEYS
            .iter()
            .map(|k| ((*k).to_string(), json!("y".repeat(ORIGIN_MAX_FIELD))))
            .collect();
        let e = sanitize_origin(&Value::Object(big)).unwrap_err();
        assert!(e.contains("exceeds"), "{e}");
    }

    #[tokio::test]
    async fn origin_and_workflow_round_trip_through_the_subscription() {
        let (db, _d) = fresh_db().await;
        let origin = sanitize_origin(&json!({
            "platform": "zulip", "chat_id": "stream:eng", "thread_id": "deploy-bug"
        }))
        .unwrap()
        .unwrap();
        let row = create_subscription(
            &db,
            "hermes-1",
            "sess-1",
            &[],
            &[],
            0,
            DEFAULT_TTL_SECS,
            &DeliverySpec::default(),
            Some(&origin),
            Some("wf-7"),
        )
        .await
        .unwrap();
        assert_eq!(row.workflow_id.as_deref(), Some("wf-7"));
        let stored = get_subscription(&db, &row.id).await.unwrap().unwrap();
        let parsed = origin_value(stored.origin.as_deref());
        assert_eq!(parsed["thread_id"], "deploy-bug");
        assert_eq!(stored.workflow_id.as_deref(), Some("wf-7"));

        // Backward compatibility: a subscription without origin/workflow works.
        let plain = sub_for(&db, "sess-2", 0).await;
        assert!(plain.origin.is_none() && plain.workflow_id.is_none());
        assert_eq!(origin_value(plain.origin.as_deref()), json!({}));
    }

    #[tokio::test]
    async fn due_deliveries_carry_workflow_and_origin() {
        let (db, _d) = fresh_db().await;
        let origin = sanitize_origin(&json!({"chat_id": "stream:eng", "thread_id": "t"}))
            .unwrap()
            .unwrap();
        create_subscription(
            &db,
            "hermes-1",
            "s1",
            &[],
            &[],
            0,
            DEFAULT_TTL_SECS,
            &DeliverySpec {
                url: Some("http://127.0.0.1:9/webhooks/pok".into()),
                secret_env: Some("POK_WEBHOOK_SECRET".into()),
                secret_file: None,
            },
            Some(&origin),
            Some("wf-9"),
        )
        .await
        .unwrap();
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let due = due_deliveries(&db, now_epoch(), 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].workflow_id.as_deref(), Some("wf-9"));
        assert_eq!(due[0].origin["chat_id"], "stream:eng");
    }

    // --- M16: webhook delivery state ---

    #[tokio::test]
    async fn poll_only_subscriptions_never_enter_the_delivery_queue() {
        let (db, _d) = fresh_db().await;
        sub_for(&db, "s1", 0).await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend[0].delivery_state, "none");
        assert!(due_deliveries(&db, now_epoch(), 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_webhook_subscription_queues_the_notification_for_push() {
        let (db, _d) = fresh_db().await;
        let sub = push_sub(&db, "s1").await;
        // The secret reference is persisted; the value never is.
        assert_eq!(
            sub.deliver_secret_env.as_deref(),
            Some("POK_WEBHOOK_SECRET")
        );
        assert!(sub.deliver_url.is_some());

        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let due = due_deliveries(&db, now_epoch(), 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].notification.seq, 5);
        assert_eq!(due[0].secret_env.as_deref(), Some("POK_WEBHOOK_SECRET"));
        assert_eq!(due[0].notification.delivery_state, "pending");
        assert_eq!(due[0].notification.delivery_attempts, 0);
    }

    #[tokio::test]
    async fn mark_delivered_does_not_ack() {
        let (db, _d) = fresh_db().await;
        push_sub(&db, "s1").await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();
        mark_delivered(&db, &id).await.unwrap();

        // Delivered → no longer due…
        assert!(due_deliveries(&db, now_epoch(), 10)
            .await
            .unwrap()
            .is_empty());
        // …but STILL pending for the agent: push told Hermes, it didn't handle it.
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend.len(), 1, "a delivered notification is still unacked");
        assert_eq!(pend[0].delivery_state, "delivered");
        assert_eq!(pend[0].delivery_attempts, 1);
    }

    #[tokio::test]
    async fn record_failure_reschedules_and_keeps_the_row_pollable() {
        let (db, _d) = fresh_db().await;
        push_sub(&db, "s1").await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();

        let retry_at = now_epoch() + 60;
        record_failure(&db, &id, "HTTP 500: boom", Some(retry_at))
            .await
            .unwrap();
        // Not due until the backoff expires…
        assert!(due_deliveries(&db, now_epoch(), 10)
            .await
            .unwrap()
            .is_empty());
        // …and due again afterwards, with the attempt counted.
        let later = due_deliveries(&db, retry_at, 10).await.unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].notification.delivery_attempts, 1);
        assert_eq!(later[0].notification.delivery_state, "pending");
        // Throughout, the cron fallback can still see it.
        assert_eq!(pending(&db, None, None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn giving_up_parks_as_failed_but_never_acks() {
        let (db, _d) = fresh_db().await;
        push_sub(&db, "s1").await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();

        record_failure(&db, &id, "gave up", None).await.unwrap();
        assert!(
            due_deliveries(&db, now_epoch() + 100_000, 10)
                .await
                .unwrap()
                .is_empty(),
            "a failed row is never retried automatically"
        );
        let pend = pending(&db, None, None, 10).await.unwrap();
        assert_eq!(pend.len(), 1, "and the cron fallback still delivers it");
        assert_eq!(pend[0].delivery_state, "failed");
    }

    #[tokio::test]
    async fn error_text_is_bounded() {
        let (db, _d) = fresh_db().await;
        push_sub(&db, "s1").await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();
        record_failure(&db, &id, &"x".repeat(10_000), Some(now_epoch() + 1))
            .await
            .unwrap();
        let stored: (Option<String>,) =
            sqlx::query_as("SELECT last_delivery_error FROM notifications WHERE id = ?1")
                .bind(&id)
                .fetch_one(&db)
                .await
                .unwrap();
        assert!(stored.0.unwrap().len() <= 300);
    }

    #[tokio::test]
    async fn acked_and_expired_rows_are_not_pushed() {
        let (db, _d) = fresh_db().await;
        // Acked before the push went out (e.g. the cron fallback won the race).
        push_sub(&db, "s1").await;
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();
        ack(&db, std::slice::from_ref(&id)).await.unwrap();
        assert!(
            due_deliveries(&db, now_epoch(), 10)
                .await
                .unwrap()
                .is_empty(),
            "an already-handled notification must not be pushed"
        );

        // Expired subscription: queued row stays, push stops.
        let expired = push_sub(&db, "s2").await;
        match_event(&db, "s2", "stop", 5, "t", &ev()).await.unwrap();
        sqlx::query("UPDATE subscriptions SET expires_at = ?1 WHERE id = ?2")
            .bind(now_epoch() - 60)
            .bind(&expired.id)
            .execute(&db)
            .await
            .unwrap();
        assert!(
            due_deliveries(&db, now_epoch(), 10)
                .await
                .unwrap()
                .is_empty(),
            "an expired subscription must not push"
        );
    }

    #[tokio::test]
    async fn pending_deliveries_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.db");
        let id = {
            let db = store::open(&path).await.unwrap();
            push_sub(&db, "s1").await;
            match_event(&db, "s1", "stop", 7, "t", &ev()).await.unwrap();
            let id = pending(&db, None, None, 10).await.unwrap()[0].id.clone();
            // One failed attempt already recorded, retry scheduled in the past.
            record_failure(&db, &id, "HTTP 503", Some(now_epoch() - 1))
                .await
                .unwrap();
            db.close().await;
            id
        };
        let db2 = store::open(&path).await.unwrap();
        let due = due_deliveries(&db2, now_epoch(), 10).await.unwrap();
        assert_eq!(due.len(), 1, "the delivery loop resumes after a restart");
        assert_eq!(due[0].notification.id, id);
        assert_eq!(due[0].notification.delivery_attempts, 1);
        assert_eq!(due[0].secret_env.as_deref(), Some("POK_WEBHOOK_SECRET"));
    }

    #[tokio::test]
    async fn due_deliveries_are_ordered_and_batched() {
        let (db, _d) = fresh_db().await;
        push_sub(&db, "s1").await;
        for seq in 1..=5 {
            match_event(&db, "s1", "stop", seq, "t", &ev())
                .await
                .unwrap();
        }
        let due = due_deliveries(&db, now_epoch(), 3).await.unwrap();
        assert_eq!(due.len(), 3, "batch limit honoured");
        assert_eq!(
            due.iter().map(|d| d.notification.seq).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "oldest first"
        );
    }

    #[tokio::test]
    async fn set_delivery_switches_modes_and_arms_queued_rows() {
        let (db, _d) = fresh_db().await;
        let sub = sub_for(&db, "s1", 0).await; // poll-only
        match_event(&db, "s1", "stop", 5, "t", &ev()).await.unwrap();
        assert!(due_deliveries(&db, now_epoch(), 10)
            .await
            .unwrap()
            .is_empty());

        // Point it at a webhook: the already-queued row becomes due.
        let spec = DeliverySpec {
            url: Some("http://127.0.0.1:9/webhooks/pok".into()),
            secret_env: Some("POK_WEBHOOK_SECRET".into()),
            secret_file: None,
        };
        let updated = set_delivery(&db, &sub.id, &spec).await.unwrap().unwrap();
        assert!(updated.deliver_url.is_some());
        assert_eq!(due_deliveries(&db, now_epoch(), 10).await.unwrap().len(), 1);

        // Clear it: pushes stop, the notification stays pollable.
        let cleared = set_delivery(&db, &sub.id, &DeliverySpec::default())
            .await
            .unwrap()
            .unwrap();
        assert!(cleared.deliver_url.is_none());
        assert!(due_deliveries(&db, now_epoch(), 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(pending(&db, None, None, 10).await.unwrap().len(), 1);

        assert!(set_delivery(&db, "sub-nope", &spec)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delivery_summary_counts_states() {
        let (db, _d) = fresh_db().await;
        let sub = push_sub(&db, "s1").await;
        for seq in 1..=3 {
            match_event(&db, "s1", "stop", seq, "t", &ev())
                .await
                .unwrap();
        }
        let ids: Vec<String> = pending(&db, None, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        mark_delivered(&db, &ids[0]).await.unwrap();
        record_failure(&db, &ids[1], "nope", None).await.unwrap();
        let sum = delivery_summary(&db, &sub.id).await.unwrap();
        assert_eq!(sum["delivered"], 1);
        assert_eq!(sum["delivery_failed"], 1);
        assert_eq!(sum["delivery_pending"], 1);
        assert_eq!(sum["unacked"], 3, "delivery state never implies handled");
    }

    #[tokio::test]
    async fn create_clamps_the_ttl_and_persists_it() {
        let (db, _d) = fresh_db().await;
        let huge = create_subscription(
            &db,
            "h",
            "s1",
            &[],
            &[],
            0,
            MAX_TTL_SECS * 10,
            &DeliverySpec::default(),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(huge.ttl_secs, MAX_TTL_SECS);
        let zero = create_subscription(
            &db,
            "h",
            "s2",
            &[],
            &[],
            0,
            0,
            &DeliverySpec::default(),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            zero.ttl_secs, 1,
            "a non-positive ttl clamps to 1s, never 0 or negative"
        );
    }
}
