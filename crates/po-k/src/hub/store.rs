//! SQLite tables for the hub: remembered hosts and session watches.

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
"#;

pub async fn apply_schema(db: &Db) -> Result<()> {
    sqlx::query(SCHEMA)
        .execute(db)
        .await
        .context("applying hub schema")?;
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
    String,
    String,
);

fn watch_from_tuple(t: WatchTuple) -> WatchRow {
    let (id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, last_event, last_error, created_at, updated_at) = t;
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
        created_at,
        updated_at,
    }
}

const WATCH_COLS: &str = "id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, last_event, last_error, created_at, updated_at";

pub async fn insert_watch(db: &Db, host: &str, sid: &str, webhook: &WebhookTarget, meta: &Value, since_boundary: i64) -> Result<WatchRow> {
    let id = format!("w-{}", uuid::Uuid::new_v4().simple());
    let now = now_iso();
    sqlx::query(
        r#"INSERT INTO hub_watches (id, host, sid, webhook_url, secret_env, secret_file, meta, since_boundary, state, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9, ?9)"#,
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
        let w = insert_watch(&db, "box", "sid-1", &wh, &json!({"thread_id": "t"}), 7).await.unwrap();
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
}
