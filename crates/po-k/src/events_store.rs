//! SQLite persistence: `sessions` table + `events` table.
//!
//! The `events` table is the only place where the orchestrator-visible event
//! stream lives. Writers: the JSONL tailer (M11.5), the hook ingest handler
//! (M11.5), the spawn / cleanup pipeline (M11.4), and the permission tracker
//! (M11.8). `seq` is monotonic *per session*.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::Path;
use std::str::FromStr;

pub type Db = Pool<Sqlite>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sessions (
    sid              TEXT PRIMARY KEY,
    project          TEXT NOT NULL,
    cwd              TEXT NOT NULL,
    zellij_session   TEXT NOT NULL,
    model            TEXT,
    effort           TEXT,
    started_at       TEXT NOT NULL,
    ended_at         TEXT,
    pid              INTEGER,
    last_event_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    sid              TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    ts               TEXT NOT NULL,
    kind             TEXT NOT NULL,
    payload          TEXT NOT NULL,
    PRIMARY KEY (sid, seq)
);
CREATE INDEX IF NOT EXISTS events_by_sid_seq ON events (sid, seq);
"#;

/// Connect (creating the file if missing) and apply the schema.
pub async fn open(path: &Path) -> Result<Db> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let opts = SqliteConnectOptions::from_str(&url)
        .with_context(|| format!("parsing sqlite url {url}"))?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await
        .context("connecting sqlite")?;
    sqlx::query(SCHEMA)
        .execute(&pool)
        .await
        .context("applying schema")?;
    Ok(pool)
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionRow {
    pub sid: String,
    pub project: String,
    pub cwd: String,
    pub zellij_session: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub pid: Option<i64>,
    pub last_event_seq: i64,
}

pub async fn insert_session(db: &Db, row: &SessionRow) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO sessions (sid, project, cwd, zellij_session, model, effort, started_at, pid, last_event_seq)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
    )
    .bind(&row.sid)
    .bind(&row.project)
    .bind(&row.cwd)
    .bind(&row.zellij_session)
    .bind(&row.model)
    .bind(&row.effort)
    .bind(&row.started_at)
    .bind(row.pid)
    .bind(row.last_event_seq)
    .execute(db)
    .await
    .context("INSERT INTO sessions")?;
    Ok(())
}

pub async fn list_sessions(db: &Db) -> Result<Vec<SessionRow>> {
    let rows: Vec<(String, String, String, String, Option<String>, Option<String>, String, Option<String>, Option<i64>, i64)> =
        sqlx::query_as(
            r#"SELECT sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq
               FROM sessions ORDER BY started_at DESC"#,
        )
        .fetch_all(db)
        .await
        .context("SELECT FROM sessions")?;
    Ok(rows
        .into_iter()
        .map(|(sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq)| SessionRow {
            sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq,
        })
        .collect())
}

pub async fn get_session(db: &Db, sid: &str) -> Result<Option<SessionRow>> {
    let row: Option<(String, String, String, String, Option<String>, Option<String>, String, Option<String>, Option<i64>, i64)> =
        sqlx::query_as(
            r#"SELECT sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq
               FROM sessions WHERE sid = ?1"#,
        )
        .bind(sid)
        .fetch_optional(db)
        .await
        .context("SELECT FROM sessions")?;
    Ok(row.map(|(sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq)| SessionRow {
        sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq,
    }))
}

pub async fn mark_session_ended(db: &Db, sid: &str, ended_at: &str) -> Result<()> {
    sqlx::query(r#"UPDATE sessions SET ended_at = ?1 WHERE sid = ?2"#)
        .bind(ended_at)
        .bind(sid)
        .execute(db)
        .await
        .context("UPDATE sessions ended_at")?;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct EventRow {
    pub sid: String,
    pub seq: i64,
    pub ts: String,
    pub kind: String,
    pub payload: Value,
}

/// Append an event in a single transaction: bump `sessions.last_event_seq`,
/// insert the row with `seq = new_seq`. Returns the assigned seq.
pub async fn append_event(
    db: &Db,
    sid: &str,
    ts: &str,
    kind: &str,
    payload: &Value,
) -> Result<i64> {
    let mut tx = db.begin().await.context("begin tx")?;
    sqlx::query(r#"UPDATE sessions SET last_event_seq = last_event_seq + 1 WHERE sid = ?1"#)
        .bind(sid)
        .execute(&mut *tx)
        .await
        .context("UPDATE last_event_seq")?;
    let (seq,): (i64,) =
        sqlx::query_as(r#"SELECT last_event_seq FROM sessions WHERE sid = ?1"#)
            .bind(sid)
            .fetch_one(&mut *tx)
            .await
            .context("SELECT last_event_seq")?;
    sqlx::query(
        r#"INSERT INTO events (sid, seq, ts, kind, payload) VALUES (?1, ?2, ?3, ?4, ?5)"#,
    )
    .bind(sid)
    .bind(seq)
    .bind(ts)
    .bind(kind)
    .bind(serde_json::to_string(payload).context("serialize payload")?)
    .execute(&mut *tx)
    .await
    .context("INSERT INTO events")?;
    tx.commit().await.context("commit tx")?;
    Ok(seq)
}

pub async fn select_events_since(
    db: &Db,
    sid: &str,
    since: i64,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM events
           WHERE sid = ?1 AND seq > ?2
           ORDER BY seq ASC LIMIT ?3"#,
    )
    .bind(sid)
    .bind(since)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT events since")?;
    Ok(rows
        .into_iter()
        .map(|(sid, seq, ts, kind, payload)| EventRow {
            sid,
            seq,
            ts,
            kind,
            payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
        })
        .collect())
}

/// UTC ISO-8601 with second precision; matches what we emit in events.
pub fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    chrono_lite::iso(secs)
}

mod chrono_lite {
    //! Tiny no-dep ISO-8601 formatter. Year/month/day from epoch seconds via
    //! the proleptic Gregorian calendar (1970-2106 safe).
    pub fn iso(epoch_secs: u64) -> String {
        let days_since_epoch = (epoch_secs / 86_400) as i64;
        let secs_of_day = (epoch_secs % 86_400) as i64;
        let hour = secs_of_day / 3600;
        let minute = (secs_of_day % 3600) / 60;
        let second = secs_of_day % 60;
        let (y, m, d) = days_to_ymd(days_since_epoch + 719_468); // 0000-03-01 era
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            y, m, d, hour, minute, second
        )
    }

    fn days_to_ymd(days: i64) -> (i64, u32, u32) {
        let era = if days >= 0 { days / 146_097 } else { (days - 146_096) / 146_097 };
        let doe = (days - era * 146_097) as u64;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = (yoe as i64) + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
        let y = if m <= 2 { y + 1 } else { y };
        (y, m, d)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn epoch_zero() {
            assert_eq!(iso(0), "1970-01-01T00:00:00Z");
        }
        #[test]
        fn known_date() {
            // 2026-05-25T12:00:00Z
            assert_eq!(iso(1779710400), "2026-05-25T12:00:00Z");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn fresh_db() -> Db {
        let path = std::env::temp_dir().join(format!("po-k-test-{}.db", uuid::Uuid::new_v4()));
        open(&path).await.unwrap()
    }

    fn row(sid: &str) -> SessionRow {
        SessionRow {
            sid: sid.into(),
            project: "p".into(),
            cwd: "/x".into(),
            zellij_session: "z".into(),
            model: Some("sonnet".into()),
            effort: Some("medium".into()),
            started_at: "2026-05-25T12:00:00Z".into(),
            ended_at: None,
            pid: Some(42),
            last_event_seq: 0,
        }
    }

    #[tokio::test]
    async fn insert_and_select_session() {
        let db = fresh_db().await;
        insert_session(&db, &row("s1")).await.unwrap();
        let got = get_session(&db, "s1").await.unwrap().unwrap();
        assert_eq!(got.project, "p");
        assert_eq!(got.pid, Some(42));
    }

    #[tokio::test]
    async fn append_assigns_monotonic_seq() {
        let db = fresh_db().await;
        insert_session(&db, &row("s2")).await.unwrap();
        let s1 = append_event(&db, "s2", "2026-05-25T12:00:00Z", "user_prompt", &json!({"text":"a"})).await.unwrap();
        let s2 = append_event(&db, "s2", "2026-05-25T12:00:01Z", "tool_use", &json!({"name":"Bash"})).await.unwrap();
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        let events = select_events_since(&db, "s2", 0, 100).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "user_prompt");
        assert_eq!(events[1].kind, "tool_use");
        let since1 = select_events_since(&db, "s2", 1, 100).await.unwrap();
        assert_eq!(since1.len(), 1);
        assert_eq!(since1[0].seq, 2);
    }

    #[tokio::test]
    async fn mark_ended_works() {
        let db = fresh_db().await;
        insert_session(&db, &row("s3")).await.unwrap();
        mark_session_ended(&db, "s3", "2026-05-25T13:00:00Z").await.unwrap();
        let got = get_session(&db, "s3").await.unwrap().unwrap();
        assert_eq!(got.ended_at.as_deref(), Some("2026-05-25T13:00:00Z"));
    }
}
