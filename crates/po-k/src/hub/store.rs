//! SQLite tables for the hub: remembered hosts, session watches, and the
//! notification log (one row per watch and boundary; delivered until acked).

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::events_store::{now_iso, Db};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS hub_hosts (
    host          TEXT PRIMARY KEY,
    base_url      TEXT NOT NULL,
    webhook_url   TEXT,
    secret_env    TEXT,
    secret_file   TEXT,
    meta          TEXT,
    added_at      TEXT NOT NULL,
    last_seen_at  TEXT,
    last_error    TEXT
);
CREATE TABLE IF NOT EXISTS hub_watches (
    id              TEXT PRIMARY KEY,
    host            TEXT NOT NULL,
    sid             TEXT NOT NULL,
    webhook_url     TEXT NOT NULL,
    secret_env      TEXT,
    secret_file     TEXT,
    meta            TEXT,
    since_boundary  INTEGER NOT NULL DEFAULT 0,
    state           TEXT NOT NULL DEFAULT 'active',
    last_event      TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS hub_watches_by_host_sid ON hub_watches (host, sid);
CREATE TABLE IF NOT EXISTS hub_notifications (
    id                 TEXT PRIMARY KEY,
    watch_id           TEXT NOT NULL,
    host               TEXT NOT NULL,
    sid                TEXT NOT NULL,
    event              TEXT NOT NULL,
    boundary_cursor    INTEGER NOT NULL,
    status             TEXT,
    deciding_event     TEXT,
    message            TEXT,
    origin             TEXT,
    requires_ack       INTEGER NOT NULL DEFAULT 1,
    state              TEXT NOT NULL DEFAULT 'pending',
    attempts           INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    TEXT,
    first_delivered_at TEXT,
    delivered_at       TEXT,
    acked_at           TEXT,
    last_error         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE (watch_id, event, boundary_cursor)
);
CREATE INDEX IF NOT EXISTS hub_notifications_due ON hub_notifications (state, next_attempt_at);
CREATE INDEX IF NOT EXISTS hub_notifications_by_sid ON hub_notifications (host, sid);
"#;

/// Additive migrations for tables that already exist (errors on re-run are expected).
const MIGRATIONS: &[&str] = &["ALTER TABLE hub_watches ADD COLUMN ack_timeout_secs INTEGER NOT NULL DEFAULT 900"];

pub async fn apply_schema(db: &Db) -> Result<()> {
    sqlx::query(SCHEMA)
        .execute(db)
        .await
        .context("applying hub schema")?;
    for m in MIGRATIONS {
        let _ = sqlx::query(m).execute(db).await;
    }
    Ok(())
}

/// Webhook target: where to POST and how to sign. The secret itself is never
/// stored — only the env var name or file path that holds it.
#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct WebhookTarget {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HostRow {
    pub host: String,
    pub base_url: String,
    pub webhook: Option<WebhookTarget>,
    pub meta: Value,
    pub added_at: String,
    pub last_seen_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WatchRow {
    pub id: String,
    pub host: String,
    pub session_id: String,
    pub webhook: WebhookTarget,
    pub meta: Value,
    pub since_boundary: i64,
    pub state: String,
    pub last_event: Option<String>,
    pub last_error: Option<String>,
    /// Replay a delivered-but-unacknowledged notification after this long.
    pub ack_timeout_secs: i64,
    pub created_at: String,
    pub updated_at: String,
}

fn parse_meta(s: Option<String>) -> Value {
    s.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or(Value::Null)
}

fn meta_text(meta: &Value) -> Option<String> {
    if meta.is_null() {
        None
    } else {
        serde_json::to_string(meta).ok()
    }
}

type HostTuple = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
);

fn host_from_tuple(t: HostTuple) -> HostRow {
    let (host, base_url, webhook_url, secret_env, secret_file, meta, added_at, last_seen_at, last_error) = t;
    HostRow {
        host,
        base_url,
        webhook: webhook_url.map(|url| WebhookTarget { url, secret_env, secret_file }),
        meta: parse_meta(meta),
        added_at,
        last_seen_at,
        last_error,
    }
}

const HOST_COLS: &str = "host, base_url, webhook_url, secret_env, secret_file, meta, added_at, last_seen_at, last_error";

pub async fn upsert_host(db: &Db, host: &str, base_url: &str, webhook: Option<&WebhookTarget>, meta: &Value) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"INSERT INTO hub_hosts (host, base_url, webhook_url, secret_env, secret_file, meta, added_at, last_seen_at, last_error)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, NULL)
           ON CONFLICT(host) DO UPDATE SET base_url = excluded.base_url, webhook_url = excluded.webhook_url,
             secret_env = excluded.secret_env, secret_file = excluded.secret_file, meta = excluded.meta,
             last_seen_at = excluded.last_seen_at, last_error = NULL"#,
    )
    .bind(host)
    .bind(base_url)
    .bind(webhook.map(|w| w.url.clone()))
    .bind(webhook.and_then(|w| w.secret_env.clone()))
    .bind(webhook.and_then(|w| w.secret_file.clone()))
    .bind(meta_text(meta))
    .bind(&now)
    .execute(db)
    .await
    .context("upsert hub_hosts")?;
    Ok(())
}

pub async fn get_host(db: &Db, host: &str) -> Result<Option<HostRow>> {
    let row: Option<HostTuple> = sqlx::query_as(&format!("SELECT {HOST_COLS} FROM hub_hosts WHERE host = ?1"))
        .bind(host)
        .fetch_optional(db)
        .await
        .context("SELECT hub_hosts")?;
    Ok(row.map(host_from_tuple))
}

pub async fn list_hosts(db: &Db) -> Result<Vec<HostRow>> {
    let rows: Vec<HostTuple> = sqlx::query_as(&format!("SELECT {HOST_COLS} FROM hub_hosts ORDER BY host"))
        .fetch_all(db)
        .await
        .context("SELECT hub_hosts")?;
    Ok(rows.into_iter().map(host_from_tuple).collect())
}

pub async fn delete_host(db: &Db, host: &str) -> Result<bool> {
    let r = sqlx::query("DELETE FROM hub_hosts WHERE host = ?1")
        .bind(host)
        .execute(db)
        .await
        .context("DELETE hub_hosts")?;
    Ok(r.rows_affected() > 0)
}

/// Record a successful (`error = None`) or failed contact with a host.
pub async fn touch_host(db: &Db, host: &str, error: Option<&str>) -> Result<()> {
    match error {
        None => sqlx::query("UPDATE hub_hosts SET last_seen_at = ?1, last_error = NULL WHERE host = ?2")
            .bind(now_iso())
            .bind(host)
            .execute(db)
            .await
            .context("touch hub_hosts")?,
        Some(e) => sqlx::query("UPDATE hub_hosts SET last_error = ?1 WHERE host = ?2")
            .bind(e)
            .bind(host)
            .execute(db)
            .await
            .context("touch hub_hosts")?,
    };
    Ok(())
}

type WatchTuple = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    String,
    Option<String>,
    Option<String>,
    i64,
    String,
    String,
);

fn watch_from_tuple(t: WatchTuple) -> WatchRow {
    let (id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, last_event, last_error, ack_timeout_secs, created_at, updated_at) = t;
    WatchRow {
        id,
        host,
        session_id: sid,
        webhook: WebhookTarget { url: webhook_url, secret_env, secret_file },
        meta: parse_meta(meta),
        since_boundary,
        state,
        last_event,
        last_error,
        ack_timeout_secs,
        created_at,
        updated_at,
    }
}

const WATCH_COLS: &str = "id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, last_event, last_error, ack_timeout_secs, created_at, updated_at";

pub async fn insert_watch(db: &Db, host: &str, sid: &str, webhook: &WebhookTarget, meta: &Value, since_boundary: i64, ack_timeout_secs: i64) -> Result<WatchRow> {
    let id = format!("w-{}", uuid::Uuid::new_v4().simple());
    let now = now_iso();
    sqlx::query(
        r#"INSERT INTO hub_watches (id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, ack_timeout_secs, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?10, ?9, ?9)"#,
    )
    .bind(&id)
    .bind(host)
    .bind(sid)
    .bind(&webhook.url)
    .bind(&webhook.secret_env)
    .bind(&webhook.secret_file)
    .bind(meta_text(meta))
    .bind(since_boundary)
    .bind(&now)
    .bind(ack_timeout_secs)
    .execute(db)
    .await
    .context("INSERT hub_watches")?;
    get_watch(db, &id).await?.context("watch vanished after insert")
}

pub async fn get_watch(db: &Db, id: &str) -> Result<Option<WatchRow>> {
    let row: Option<WatchTuple> = sqlx::query_as(&format!("SELECT {WATCH_COLS} FROM hub_watches WHERE id = ?1"))
        .bind(id)
        .fetch_optional(db)
        .await
        .context("SELECT hub_watches")?;
    Ok(row.map(watch_from_tuple))
}

pub async fn find_active_watch(db: &Db, host: &str, sid: &str) -> Result<Option<WatchRow>> {
    let row: Option<WatchTuple> = sqlx::query_as(&format!(
        "SELECT {WATCH_COLS} FROM hub_watches WHERE host = ?1 AND sid = ?2 AND state = 'active' ORDER BY created_at DESC LIMIT 1"
    ))
    .bind(host)
    .bind(sid)
    .fetch_optional(db)
    .await
    .context("SELECT hub_watches")?;
    Ok(row.map(watch_from_tuple))
}

pub async fn list_watches(db: &Db, host: Option<&str>, state: Option<&str>) -> Result<Vec<WatchRow>> {
    let rows: Vec<WatchTuple> = sqlx::query_as(&format!(
        "SELECT {WATCH_COLS} FROM hub_watches WHERE (?1 IS NULL OR host = ?1) AND (?2 IS NULL OR state = ?2) ORDER BY created_at DESC"
    ))
    .bind(host)
    .bind(state)
    .fetch_all(db)
    .await
    .context("SELECT hub_watches")?;
    Ok(rows.into_iter().map(watch_from_tuple).collect())
}

pub async fn active_watches(db: &Db) -> Result<Vec<WatchRow>> {
    list_watches(db, None, Some("active")).await
}

/// Advance a watch after a delivered boundary.
pub async fn record_progress(db: &Db, id: &str, since_boundary: i64, last_event: &str) -> Result<()> {
    sqlx::query("UPDATE hub_watches SET since_boundary = ?1, last_event = ?2, last_error = NULL, updated_at = ?3 WHERE id = ?4")
        .bind(since_boundary)
        .bind(last_event)
        .bind(now_iso())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_watches progress")?;
    Ok(())
}

pub async fn record_error(db: &Db, id: &str, error: &str) -> Result<()> {
    sqlx::query("UPDATE hub_watches SET last_error = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(error)
        .bind(now_iso())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_watches error")?;
    Ok(())
}

pub async fn set_state(db: &Db, id: &str, state: &str, last_event: Option<&str>, error: Option<&str>) -> Result<()> {
    sqlx::query(
        "UPDATE hub_watches SET state = ?1, last_event = COALESCE(?2, last_event), last_error = ?3, updated_at = ?4 WHERE id = ?5",
    )
    .bind(state)
    .bind(last_event)
    .bind(error)
    .bind(now_iso())
    .bind(id)
    .execute(db)
    .await
    .context("UPDATE hub_watches state")?;
    Ok(())
}

#[allow(dead_code)]
pub async fn delete_watch(db: &Db, id: &str) -> Result<bool> {
    let r = sqlx::query("DELETE FROM hub_watches WHERE id = ?1")
        .bind(id)
        .execute(db)
        .await
        .context("DELETE hub_watches")?;
    Ok(r.rows_affected() > 0)
}


// ---- notifications --------------------------------------------------------

/// Events that need the orchestrator to confirm it handled them; everything
/// else is informational and is auto-acked once delivered.
pub const ACK_EVENTS: &[&str] = &["finished", "needs_input", "ended", "session_lost"];

pub fn requires_ack(event: &str) -> bool {
    ACK_EVENTS.contains(&event)
}

#[derive(Debug, Clone, Serialize)]
pub struct NotificationRow {
    pub id: String,
    pub watch_id: String,
    pub host: String,
    pub session_id: String,
    pub event: String,
    pub boundary_cursor: i64,
    pub status: Option<String>,
    pub deciding_event: Value,
    pub message: Option<String>,
    pub origin: Value,
    pub requires_ack: bool,
    /// pending | delivered | acked | failed | cancelled
    pub state: String,
    pub attempts: i64,
    pub next_attempt_at: Option<String>,
    pub first_delivered_at: Option<String>,
    pub delivered_at: Option<String>,
    pub acked_at: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(sqlx::FromRow)]
struct NotifRaw {
    id: String,
    watch_id: String,
    host: String,
    sid: String,
    event: String,
    boundary_cursor: i64,
    status: Option<String>,
    deciding_event: Option<String>,
    message: Option<String>,
    origin: Option<String>,
    requires_ack: i64,
    state: String,
    attempts: i64,
    next_attempt_at: Option<String>,
    first_delivered_at: Option<String>,
    delivered_at: Option<String>,
    acked_at: Option<String>,
    last_error: Option<String>,
    created_at: String,
    updated_at: String,
}

const NOTIF_COLS: &str = "id, watch_id, host, sid, event, boundary_cursor, status, deciding_event, message, origin, requires_ack, state, attempts, next_attempt_at, first_delivered_at, delivered_at, acked_at, last_error, created_at, updated_at";

fn notif_from_raw(r: NotifRaw) -> NotificationRow {
    NotificationRow {
        id: r.id,
        watch_id: r.watch_id,
        host: r.host,
        session_id: r.sid,
        event: r.event,
        boundary_cursor: r.boundary_cursor,
        status: r.status,
        deciding_event: parse_meta(r.deciding_event),
        message: r.message,
        origin: parse_meta(r.origin),
        requires_ack: r.requires_ack != 0,
        state: r.state,
        attempts: r.attempts,
        next_attempt_at: r.next_attempt_at,
        first_delivered_at: r.first_delivered_at,
        delivered_at: r.delivered_at,
        acked_at: r.acked_at,
        last_error: r.last_error,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

/// Persist a boundary/health event for a watch. Boundary events are unique per
/// `(watch, event, boundary)`: a repeat returns `Ok(None)` and nothing is
/// delivered twice. Informational events may recur (a lost/restored cycle), so
/// a finished earlier row with the same key is replaced.
#[allow(clippy::too_many_arguments)]
pub async fn enqueue(
    db: &Db,
    watch: &WatchRow,
    event: &str,
    status: Option<&str>,
    boundary: i64,
    deciding: &Value,
    message: Option<&str>,
    origin: &Value,
) -> Result<Option<NotificationRow>> {
    let needs_ack = requires_ack(event);
    if !needs_ack {
        sqlx::query(
            "DELETE FROM hub_notifications WHERE watch_id = ?1 AND event = ?2 AND boundary_cursor = ?3 AND state IN ('acked','failed','cancelled')",
        )
        .bind(&watch.id)
        .bind(event)
        .bind(boundary)
        .execute(db)
        .await
        .context("clearing finished informational notification")?;
    }
    let id = format!("n-{}", uuid::Uuid::new_v4().simple());
    let now = now_iso();
    let r = sqlx::query(
        r#"INSERT OR IGNORE INTO hub_notifications
           (id, watch_id, host, sid, event, boundary_cursor, status, deciding_event, message, origin, requires_ack, state, attempts, next_attempt_at, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'pending', 0, ?12, ?12, ?12)"#,
    )
    .bind(&id)
    .bind(&watch.id)
    .bind(&watch.host)
    .bind(&watch.session_id)
    .bind(event)
    .bind(boundary)
    .bind(status)
    .bind(meta_text(deciding))
    .bind(message)
    .bind(meta_text(origin))
    .bind(if needs_ack { 1 } else { 0 })
    .bind(&now)
    .execute(db)
    .await
    .context("INSERT hub_notifications")?;
    if r.rows_affected() == 0 {
        return Ok(None);
    }
    get_notification(db, &id).await
}

pub async fn get_notification(db: &Db, id: &str) -> Result<Option<NotificationRow>> {
    let row: Option<NotifRaw> = sqlx::query_as(&format!("SELECT {NOTIF_COLS} FROM hub_notifications WHERE id = ?1"))
        .bind(id)
        .fetch_optional(db)
        .await
        .context("SELECT hub_notifications")?;
    Ok(row.map(notif_from_raw))
}

/// Rows the deliverer must act on now: never-delivered pending rows whose
/// backoff elapsed, and delivered rows that still need an ack past their
/// replay time.
pub async fn due_notifications(db: &Db, now: &str, limit: i64) -> Result<Vec<NotificationRow>> {
    let rows: Vec<NotifRaw> = sqlx::query_as(&format!(
        "SELECT {NOTIF_COLS} FROM hub_notifications
         WHERE ((state = 'pending') OR (state = 'delivered' AND requires_ack = 1))
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?1)
         ORDER BY created_at ASC LIMIT ?2"
    ))
    .bind(now)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT due hub_notifications")?;
    Ok(rows.into_iter().map(notif_from_raw).collect())
}

/// `state`: `unacked` (pending or delivered, needing ack), `pending`,
/// `delivered`, `acked`, `failed`, `cancelled`, or `all`.
pub async fn list_notifications(db: &Db, state: &str, host: Option<&str>, sid: Option<&str>, limit: i64) -> Result<Vec<NotificationRow>> {
    let (s1, s2): (Option<&str>, Option<&str>) = match state {
        "all" => (None, None),
        "unacked" => (Some("pending"), Some("delivered")),
        other => (Some(other), Some(other)),
    };
    let rows: Vec<NotifRaw> = sqlx::query_as(&format!(
        "SELECT {NOTIF_COLS} FROM hub_notifications
         WHERE (?1 IS NULL OR state = ?1 OR state = ?2)
           AND (?3 IS NULL OR host = ?3) AND (?4 IS NULL OR sid = ?4)
           AND (?5 = 0 OR requires_ack = 1)
         ORDER BY created_at DESC LIMIT ?6"
    ))
    .bind(s1)
    .bind(s2)
    .bind(host)
    .bind(sid)
    .bind(if state == "unacked" { 1 } else { 0 })
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT hub_notifications")?;
    Ok(rows.into_iter().map(notif_from_raw).collect())
}

/// Ids of other unacknowledged boundary notifications for the same watch.
pub async fn unacked_ids_for_watch(db: &Db, watch_id: &str, except: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM hub_notifications WHERE watch_id = ?1 AND id != ?2 AND requires_ack = 1 AND state IN ('pending','delivered') ORDER BY created_at",
    )
    .bind(watch_id)
    .bind(except)
    .fetch_all(db)
    .await
    .context("SELECT unacked hub_notifications")?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// A successful POST. Informational rows are done; boundary rows wait for an
/// ack and are replayed at `next_attempt_at`.
pub async fn mark_delivered(db: &Db, id: &str, requires_ack: bool, next_attempt_at: &str) -> Result<()> {
    let now = now_iso();
    if requires_ack {
        sqlx::query(
            "UPDATE hub_notifications SET state = 'delivered', attempts = attempts + 1, delivered_at = ?1,
                    first_delivered_at = COALESCE(first_delivered_at, ?1), next_attempt_at = ?2, last_error = NULL, updated_at = ?1
             WHERE id = ?3",
        )
        .bind(&now)
        .bind(next_attempt_at)
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications delivered")?;
    } else {
        sqlx::query(
            "UPDATE hub_notifications SET state = 'acked', attempts = attempts + 1, delivered_at = ?1,
                    first_delivered_at = COALESCE(first_delivered_at, ?1), acked_at = ?1, next_attempt_at = NULL, last_error = NULL, updated_at = ?1
             WHERE id = ?2",
        )
        .bind(&now)
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications auto-acked")?;
    }
    Ok(())
}

/// A transient failure: keep the current state, bump attempts, schedule a retry.
pub async fn mark_retry(db: &Db, id: &str, error: &str, next_attempt_at: &str) -> Result<()> {
    sqlx::query("UPDATE hub_notifications SET attempts = attempts + 1, next_attempt_at = ?1, last_error = ?2, updated_at = ?3 WHERE id = ?4")
        .bind(next_attempt_at)
        .bind(error)
        .bind(now_iso())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications retry")?;
    Ok(())
}

pub async fn mark_failed(db: &Db, id: &str, error: &str) -> Result<()> {
    sqlx::query("UPDATE hub_notifications SET state = 'failed', attempts = attempts + 1, next_attempt_at = NULL, last_error = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(error)
        .bind(now_iso())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications failed")?;
    Ok(())
}

/// Acknowledge. Returns `Ok(Some(true))` when this call acked it,
/// `Ok(Some(false))` when it was already acked, `Ok(None)` when unknown.
pub async fn ack(db: &Db, id: &str) -> Result<Option<bool>> {
    let Some(row) = get_notification(db, id).await? else {
        return Ok(None);
    };
    if row.state == "acked" {
        return Ok(Some(false));
    }
    sqlx::query("UPDATE hub_notifications SET state = 'acked', acked_at = ?1, next_attempt_at = NULL, updated_at = ?1 WHERE id = ?2")
        .bind(now_iso())
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications ack")?;
    Ok(Some(true))
}

/// Stop delivering anything outstanding for a watch (unwatch / disconnect).
pub async fn cancel_for_watch(db: &Db, watch_id: &str) -> Result<u64> {
    let r = sqlx::query("UPDATE hub_notifications SET state = 'cancelled', next_attempt_at = NULL, updated_at = ?1 WHERE watch_id = ?2 AND state IN ('pending','delivered')")
        .bind(now_iso())
        .bind(watch_id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications cancel")?;
    Ok(r.rows_affected())
}

/// Test/ops helper: make a row due immediately.
#[cfg(test)]
pub async fn force_due(db: &Db, id: &str) -> Result<()> {
    sqlx::query("UPDATE hub_notifications SET next_attempt_at = '1970-01-01T00:00:00Z' WHERE id = ?1")
        .bind(id)
        .execute(db)
        .await
        .context("UPDATE hub_notifications force_due")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn fresh_db() -> Db {
        let path = std::env::temp_dir().join(format!("po-k-hub-{}.db", uuid::Uuid::new_v4()));
        crate::events_store::open(&path).await.unwrap()
    }

    #[tokio::test]
    async fn hosts_round_trip() {
        let db = fresh_db().await;
        let wh = WebhookTarget { url: "http://127.0.0.1:8644/webhooks/pok".into(), secret_env: Some("S".into()), secret_file: None };
        upsert_host(&db, "ange", "http://ange.zrz:13658", Some(&wh), &json!({"chat_id": "x"})).await.unwrap();
        let h = get_host(&db, "ange").await.unwrap().unwrap();
        assert_eq!(h.base_url, "http://ange.zrz:13658");
        assert_eq!(h.webhook, Some(wh));
        assert_eq!(h.meta["chat_id"], "x");
        // Upsert replaces the webhook and clears the error.
        touch_host(&db, "ange", Some("boom")).await.unwrap();
        assert_eq!(get_host(&db, "ange").await.unwrap().unwrap().last_error.as_deref(), Some("boom"));
        upsert_host(&db, "ange", "http://ange.zrz:13658", None, &Value::Null).await.unwrap();
        let h = get_host(&db, "ange").await.unwrap().unwrap();
        assert!(h.webhook.is_none());
        assert!(h.last_error.is_none());
        assert_eq!(list_hosts(&db).await.unwrap().len(), 1);
        assert!(delete_host(&db, "ange").await.unwrap());
        assert!(!delete_host(&db, "ange").await.unwrap());
    }

    #[tokio::test]
    async fn watches_round_trip() {
        let db = fresh_db().await;
        let wh = WebhookTarget { url: "http://h/w".into(), secret_env: Some("S".into()), secret_file: None };
        let w = insert_watch(&db, "box", "sid-1", &wh, &json!({"thread_id": "t"}), 7, 900).await.unwrap();
        assert_eq!(w.ack_timeout_secs, 900);
        assert_eq!(w.state, "active");
        assert_eq!(w.since_boundary, 7);
        assert_eq!(find_active_watch(&db, "box", "sid-1").await.unwrap().unwrap().id, w.id);
        record_progress(&db, &w.id, 12, "finished").await.unwrap();
        let got = get_watch(&db, &w.id).await.unwrap().unwrap();
        assert_eq!(got.since_boundary, 12);
        assert_eq!(got.last_event.as_deref(), Some("finished"));
        record_error(&db, &w.id, "unreachable").await.unwrap();
        assert_eq!(get_watch(&db, &w.id).await.unwrap().unwrap().last_error.as_deref(), Some("unreachable"));
        set_state(&db, &w.id, "done", Some("ended"), None).await.unwrap();
        assert!(find_active_watch(&db, "box", "sid-1").await.unwrap().is_none());
        assert_eq!(list_watches(&db, Some("box"), None).await.unwrap().len(), 1);
        assert!(active_watches(&db).await.unwrap().is_empty());
        assert!(delete_watch(&db, &w.id).await.unwrap());
    }

    #[tokio::test]
    async fn notifications_are_unique_per_boundary_and_track_ack_state() {
        let db = fresh_db().await;
        let wh = WebhookTarget { url: "http://h/w".into(), secret_env: Some("S".into()), secret_file: None };
        let w = insert_watch(&db, "box", "sid-1", &wh, &json!({"chat_id": "c"}), 0, 60).await.unwrap();
        let origin = json!({"chat_id": "c"});
        let n = enqueue(&db, &w, "finished", Some("idle"), 5, &json!({"kind": "stop", "seq": 5}), None, &origin).await.unwrap().unwrap();
        assert_eq!(n.state, "pending");
        assert!(n.requires_ack);
        // Same boundary again → ignored.
        assert!(enqueue(&db, &w, "finished", Some("idle"), 5, &Value::Null, None, &origin).await.unwrap().is_none());
        // Different boundary → new row.
        let n2 = enqueue(&db, &w, "finished", Some("idle"), 9, &Value::Null, None, &origin).await.unwrap().unwrap();
        let due = due_notifications(&db, &now_iso(), 10).await.unwrap();
        assert_eq!(due.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![n.id.clone(), n2.id.clone()]);

        // Delivered → waits for ack, replay scheduled in the future.
        mark_delivered(&db, &n.id, true, &crate::events_store::iso_in(600)).await.unwrap();
        let got = get_notification(&db, &n.id).await.unwrap().unwrap();
        assert_eq!(got.state, "delivered");
        assert_eq!(got.attempts, 1);
        assert!(got.first_delivered_at.is_some());
        assert!(!due_notifications(&db, &now_iso(), 10).await.unwrap().iter().any(|r| r.id == n.id));
        force_due(&db, &n.id).await.unwrap();
        assert!(due_notifications(&db, &now_iso(), 10).await.unwrap().iter().any(|r| r.id == n.id));
        assert_eq!(unacked_ids_for_watch(&db, &w.id, &n.id).await.unwrap(), vec![n2.id.clone()]);

        // Ack is idempotent and removes it from the unacked list.
        assert_eq!(ack(&db, &n.id).await.unwrap(), Some(true));
        assert_eq!(ack(&db, &n.id).await.unwrap(), Some(false));
        assert_eq!(ack(&db, "n-nope").await.unwrap(), None);
        let unacked = list_notifications(&db, "unacked", None, None, 50).await.unwrap();
        assert_eq!(unacked.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![n2.id.clone()]);
        assert_eq!(list_notifications(&db, "acked", Some("box"), Some("sid-1"), 50).await.unwrap().len(), 1);

        // Informational events auto-ack on delivery and may recur after finishing.
        let c = enqueue(&db, &w, "connection_lost", None, 9, &Value::Null, Some("down"), &origin).await.unwrap().unwrap();
        assert!(!c.requires_ack);
        mark_delivered(&db, &c.id, false, "").await.unwrap();
        assert_eq!(get_notification(&db, &c.id).await.unwrap().unwrap().state, "acked");
        assert!(enqueue(&db, &w, "connection_lost", None, 9, &Value::Null, Some("down again"), &origin).await.unwrap().is_some());

        // Failure paths and cancellation.
        mark_retry(&db, &n2.id, "boom", &crate::events_store::iso_in(30)).await.unwrap();
        assert_eq!(get_notification(&db, &n2.id).await.unwrap().unwrap().attempts, 1);
        assert_eq!(cancel_for_watch(&db, &w.id).await.unwrap(), 2); // n2 + the recurring connection_lost
        assert_eq!(get_notification(&db, &n2.id).await.unwrap().unwrap().state, "cancelled");
        let n3 = enqueue(&db, &w, "ended", Some("ended"), 12, &Value::Null, None, &origin).await.unwrap().unwrap();
        mark_failed(&db, &n3.id, "HTTP 401").await.unwrap();
        assert_eq!(list_notifications(&db, "failed", None, None, 10).await.unwrap()[0].last_error.as_deref(), Some("HTTP 401"));
    }
}
