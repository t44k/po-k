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
use std::collections::HashMap;
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
    // Additive migrations. `CREATE TABLE IF NOT EXISTS` doesn't update an
    // existing table, so add new columns separately and tolerate the
    // "duplicate column name" error on second+ run.
    let _ = sqlx::query(
        "ALTER TABLE sessions ADD COLUMN last_jsonl_offset INTEGER NOT NULL DEFAULT 0",
    )
    .execute(&pool)
    .await;
    // Profile system (M14): names of the profiles merged into this session
    // (JSON array) and the path to the generated CC plugin directory.
    let _ = sqlx::query("ALTER TABLE sessions ADD COLUMN profiles TEXT")
        .execute(&pool)
        .await;
    let _ = sqlx::query("ALTER TABLE sessions ADD COLUMN plugin_dir TEXT")
        .execute(&pool)
        .await;
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
    /// JSON array of profile names merged into this session (M14). None for
    /// legacy/profile-less sessions.
    #[serde(default)]
    pub profiles: Option<String>,
    /// Path to the generated CC plugin directory (M14). None for legacy
    /// sessions launched without a profile.
    #[serde(default)]
    pub plugin_dir: Option<String>,
}

/// Column list shared by every `SELECT … FROM sessions`, in struct order.
const SESSION_COLS: &str = "sid, project, cwd, zellij_session, model, effort, started_at, ended_at, pid, last_event_seq, profiles, plugin_dir";

type SessionTuple = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<i64>,
    i64,
    Option<String>,
    Option<String>,
);

fn row_from_tuple(t: SessionTuple) -> SessionRow {
    let (
        sid,
        project,
        cwd,
        zellij_session,
        model,
        effort,
        started_at,
        ended_at,
        pid,
        last_event_seq,
        profiles,
        plugin_dir,
    ) = t;
    SessionRow {
        sid,
        project,
        cwd,
        zellij_session,
        model,
        effort,
        started_at,
        ended_at,
        pid,
        last_event_seq,
        profiles,
        plugin_dir,
    }
}

pub async fn insert_session(db: &Db, row: &SessionRow) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO sessions (sid, project, cwd, zellij_session, model, effort, started_at, pid, last_event_seq, profiles, plugin_dir)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"#,
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
    .bind(&row.profiles)
    .bind(&row.plugin_dir)
    .execute(db)
    .await
    .context("INSERT INTO sessions")?;
    Ok(())
}

#[allow(dead_code)]
pub async fn list_sessions(db: &Db) -> Result<Vec<SessionRow>> {
    let rows: Vec<SessionTuple> = sqlx::query_as(&format!(
        "SELECT {SESSION_COLS} FROM sessions ORDER BY started_at DESC"
    ))
    .fetch_all(db)
    .await
    .context("SELECT FROM sessions")?;
    Ok(rows.into_iter().map(row_from_tuple).collect())
}

pub async fn get_session(db: &Db, sid: &str) -> Result<Option<SessionRow>> {
    let row: Option<SessionTuple> = sqlx::query_as(&format!(
        "SELECT {SESSION_COLS} FROM sessions WHERE sid = ?1"
    ))
    .bind(sid)
    .fetch_optional(db)
    .await
    .context("SELECT FROM sessions")?;
    Ok(row.map(row_from_tuple))
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

/// Map a raw `(sid, seq, ts, kind, payload)` row into an [`EventRow`], parsing
/// the stored JSON payload (defaulting to `null` on malformed JSON). Shared by
/// every `select_*` query so they decode rows identically.
fn event_row_from_tuple(t: (String, i64, String, String, String)) -> EventRow {
    let (sid, seq, ts, kind, payload) = t;
    EventRow {
        sid,
        seq,
        ts,
        kind,
        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
    }
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
    Ok(rows.into_iter().map(event_row_from_tuple).collect())
}

/// Transcript-only events (the conversational record an orchestrator consumes)
/// with `seq > since`, ASC, capped at `limit`. Filters at the SQL level so a
/// full page is `limit` *transcript* rows rather than mostly lifecycle/raw_*
/// noise — keeping `/messages` pagination correct.
pub async fn select_messages_since(
    db: &Db,
    sid: &str,
    since: i64,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM events
           WHERE sid = ?1 AND seq > ?2
             AND kind IN ('user_prompt','assistant_message','tool_use','tool_result','turn_end')
           ORDER BY seq ASC LIMIT ?3"#,
    )
    .bind(sid)
    .bind(since)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT messages since")?;
    Ok(rows.into_iter().map(event_row_from_tuple).collect())
}

/// Latest `limit` events for a session, returned in ascending seq order.
/// Used for the `offset=-1` tail page so a caller need not know the cursor.
pub async fn select_events_tail(db: &Db, sid: &str, limit: i64) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM (
             SELECT sid, seq, ts, kind, payload FROM events
             WHERE sid = ?1
             ORDER BY seq DESC LIMIT ?2
           ) ORDER BY seq ASC"#,
    )
    .bind(sid)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT events tail")?;
    Ok(rows.into_iter().map(event_row_from_tuple).collect())
}

/// Transcript-only tail (`/messages` view). Same kind filter as
/// [`select_messages_since`].
pub async fn select_messages_tail(db: &Db, sid: &str, limit: i64) -> Result<Vec<EventRow>> {
    let rows: Vec<(String, i64, String, String, String)> = sqlx::query_as(
        r#"SELECT sid, seq, ts, kind, payload FROM (
             SELECT sid, seq, ts, kind, payload FROM events
             WHERE sid = ?1
               AND kind IN ('user_prompt','assistant_message','tool_use','tool_result','turn_end')
             ORDER BY seq DESC LIMIT ?2
           ) ORDER BY seq ASC"#,
    )
    .bind(sid)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("SELECT messages tail")?;
    Ok(rows.into_iter().map(event_row_from_tuple).collect())
}

/// The latest `seq` of each status-relevant event kind for a session, in one
/// round-trip. Feeds [`crate::status::derive_status`].
pub async fn latest_status_seqs(db: &Db, sid: &str) -> Result<HashMap<String, i64>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT kind, MAX(seq) FROM events
           WHERE sid = ?1
             AND kind IN (
               'user_prompt','assistant_message','tool_use','tool_result',
               'stop','subagent_stop','notification',
               'session_end','cc_exited','permission_request','permission_decision'
             )
           GROUP BY kind"#,
    )
    .bind(sid)
    .fetch_all(db)
    .await
    .context("SELECT latest_status_seqs")?;
    Ok(rows.into_iter().collect())
}

/// The session's current max event seq (the canonical monotonic cursor), or
/// `None` if the session row doesn't exist.
pub async fn current_cursor(db: &Db, sid: &str) -> Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as(r#"SELECT last_event_seq FROM sessions WHERE sid = ?1"#)
            .bind(sid)
            .fetch_optional(db)
            .await
            .context("SELECT last_event_seq")?;
    Ok(row.map(|(seq,)| seq))
}

/// Sessions po-k still thinks are running (`ended_at IS NULL`). The recovery
/// pass on startup walks these and reconciles them against zellij.
pub async fn unended_sessions(db: &Db) -> Result<Vec<SessionRow>> {
    let rows: Vec<SessionTuple> = sqlx::query_as(&format!(
        "SELECT {SESSION_COLS} FROM sessions WHERE ended_at IS NULL ORDER BY started_at"
    ))
    .fetch_all(db)
    .await
    .context("SELECT FROM sessions WHERE ended_at IS NULL")?;
    Ok(rows.into_iter().map(row_from_tuple).collect())
}

/// The byte position the JSONL tailer left off at for this session, or 0 if
/// unknown. Used to resume the tailer past already-ingested lines after a
/// po-k restart.
pub async fn get_jsonl_offset(db: &Db, sid: &str) -> Result<i64> {
    let row: Option<(i64,)> =
        sqlx::query_as(r#"SELECT last_jsonl_offset FROM sessions WHERE sid = ?1"#)
            .bind(sid)
            .fetch_optional(db)
            .await
            .context("SELECT last_jsonl_offset")?;
    Ok(row.map(|(o,)| o).unwrap_or(0))
}

/// Just advance the JSONL offset (no event append). Used for blank /
/// unprojectable lines the tailer must still skip past on restart.
pub async fn set_jsonl_offset(db: &Db, sid: &str, offset: i64) -> Result<()> {
    sqlx::query(r#"UPDATE sessions SET last_jsonl_offset = ?1 WHERE sid = ?2"#)
        .bind(offset)
        .bind(sid)
        .execute(db)
        .await
        .context("UPDATE last_jsonl_offset")?;
    Ok(())
}

/// Append a JSONL-tailer event AND advance `last_jsonl_offset` atomically.
/// Doing both in one transaction means a crash between them can't leave the
/// tailer in a state where it re-ingests the same line on restart.
pub async fn append_jsonl_event(
    db: &Db,
    sid: &str,
    ts: &str,
    kind: &str,
    payload: &Value,
    new_offset: i64,
) -> Result<i64> {
    let mut tx = db.begin().await.context("begin tx")?;
    sqlx::query(r#"UPDATE sessions SET last_event_seq = last_event_seq + 1, last_jsonl_offset = ?2 WHERE sid = ?1"#)
        .bind(sid)
        .bind(new_offset)
        .execute(&mut *tx)
        .await
        .context("UPDATE last_event_seq + last_jsonl_offset")?;
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
            profiles: None,
            plugin_dir: None,
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
    async fn profiles_and_plugin_dir_round_trip() {
        let db = fresh_db().await;
        let mut r = row("pf1");
        r.profiles = Some(r#"["base","reviewer"]"#.into());
        r.plugin_dir = Some("/home/me/.cache/po-k/sessions/pf1/plugin".into());
        insert_session(&db, &r).await.unwrap();
        let got = get_session(&db, "pf1").await.unwrap().unwrap();
        assert_eq!(got.profiles.as_deref(), Some(r#"["base","reviewer"]"#));
        assert_eq!(
            got.plugin_dir.as_deref(),
            Some("/home/me/.cache/po-k/sessions/pf1/plugin")
        );
        // Legacy rows (NULL columns) still deserialize.
        insert_session(&db, &row("pf2")).await.unwrap();
        let legacy = get_session(&db, "pf2").await.unwrap().unwrap();
        assert_eq!(legacy.profiles, None);
        assert_eq!(legacy.plugin_dir, None);
    }

    #[tokio::test]
    async fn mark_ended_works() {
        let db = fresh_db().await;
        insert_session(&db, &row("s3")).await.unwrap();
        mark_session_ended(&db, "s3", "2026-05-25T13:00:00Z").await.unwrap();
        let got = get_session(&db, "s3").await.unwrap().unwrap();
        assert_eq!(got.ended_at.as_deref(), Some("2026-05-25T13:00:00Z"));
    }

    #[tokio::test]
    async fn current_cursor_tracks_last_event_seq() {
        let db = fresh_db().await;
        assert_eq!(current_cursor(&db, "nope").await.unwrap(), None);
        insert_session(&db, &row("s4")).await.unwrap();
        assert_eq!(current_cursor(&db, "s4").await.unwrap(), Some(0));
        append_event(&db, "s4", "t", "user_prompt", &json!({})).await.unwrap();
        append_event(&db, "s4", "t", "stop", &json!({})).await.unwrap();
        assert_eq!(current_cursor(&db, "s4").await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn latest_status_seqs_groups_and_excludes() {
        let db = fresh_db().await;
        insert_session(&db, &row("s5")).await.unwrap();
        append_event(&db, "s5", "t", "user_prompt", &json!({})).await.unwrap(); // 1
        append_event(&db, "s5", "t", "raw", &json!({})).await.unwrap();          // 2 (excluded)
        append_event(&db, "s5", "t", "tool_use", &json!({})).await.unwrap();     // 3
        append_event(&db, "s5", "t", "stop", &json!({})).await.unwrap();         // 4
        // idle_prompt notifications are remapped to this kind at hook
        // ingestion; it must stay outside the status-relevant IN-clause.
        append_event(&db, "s5", "t", "idle_notification", &json!({})).await.unwrap(); // 5 (excluded)
        let latest = latest_status_seqs(&db, "s5").await.unwrap();
        assert_eq!(latest.get("user_prompt"), Some(&1));
        assert_eq!(latest.get("tool_use"), Some(&3));
        assert_eq!(latest.get("stop"), Some(&4));
        assert_eq!(latest.get("raw"), None); // not a status-relevant kind
        assert_eq!(latest.get("idle_notification"), None); // never drives awaiting_input
    }

    #[tokio::test]
    async fn unended_sessions_filters_correctly() {
        let db = fresh_db().await;
        insert_session(&db, &row("alive1")).await.unwrap();
        insert_session(&db, &row("alive2")).await.unwrap();
        insert_session(&db, &row("dead")).await.unwrap();
        mark_session_ended(&db, "dead", "2026-05-29T01:00:00Z").await.unwrap();
        let unended: Vec<String> = unended_sessions(&db).await.unwrap().into_iter().map(|s| s.sid).collect();
        assert!(unended.contains(&"alive1".to_string()));
        assert!(unended.contains(&"alive2".to_string()));
        assert!(!unended.contains(&"dead".to_string()));
    }

    #[tokio::test]
    async fn append_jsonl_event_bumps_both_counters_atomically() {
        let db = fresh_db().await;
        insert_session(&db, &row("j1")).await.unwrap();
        let seq1 = append_jsonl_event(&db, "j1", "t", "user_prompt", &json!({"text":"a"}), 120).await.unwrap();
        let seq2 = append_jsonl_event(&db, "j1", "t", "assistant_message", &json!({"text":"b"}), 250).await.unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(current_cursor(&db, "j1").await.unwrap(), Some(2));
        assert_eq!(get_jsonl_offset(&db, "j1").await.unwrap(), 250);
        // Plain append_event still works alongside; offset does NOT advance.
        let seq3 = append_event(&db, "j1", "t", "stop", &json!({})).await.unwrap();
        assert_eq!(seq3, 3);
        assert_eq!(get_jsonl_offset(&db, "j1").await.unwrap(), 250);
    }

    #[tokio::test]
    async fn select_messages_since_filters_to_transcript() {
        let db = fresh_db().await;
        insert_session(&db, &row("s6")).await.unwrap();
        append_event(&db, "s6", "t", "user_prompt", &json!({"text":"hi"})).await.unwrap(); // 1
        append_event(&db, "s6", "t", "notification", &json!({})).await.unwrap();            // 2 (excluded)
        append_event(&db, "s6", "t", "assistant_message", &json!({"text":"yo"})).await.unwrap(); // 3
        append_event(&db, "s6", "t", "permission_request", &json!({})).await.unwrap();      // 4 (excluded)
        append_event(&db, "s6", "t", "turn_end", &json!({})).await.unwrap();                // 5
        let msgs = select_messages_since(&db, "s6", 0, 100).await.unwrap();
        let kinds: Vec<&str> = msgs.iter().map(|r| r.kind.as_str()).collect();
        assert_eq!(kinds, vec!["user_prompt", "assistant_message", "turn_end"]);
        // `since` cursor and ordering hold.
        let after = select_messages_since(&db, "s6", 1, 100).await.unwrap();
        assert_eq!(after.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 5]);
    }

    #[tokio::test]
    async fn select_events_tail_returns_latest_in_order() {
        let db = fresh_db().await;
        insert_session(&db, &row("t1")).await.unwrap();
        for _ in 0..20 {
            append_event(&db, "t1", "t", "user_prompt", &json!({})).await.unwrap();
        }
        let tail = select_events_tail(&db, "t1", 5).await.unwrap();
        assert_eq!(
            tail.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![16, 17, 18, 19, 20]
        );
    }

    #[tokio::test]
    async fn select_events_tail_when_fewer_than_limit() {
        let db = fresh_db().await;
        insert_session(&db, &row("t2")).await.unwrap();
        for _ in 0..3 {
            append_event(&db, "t2", "t", "user_prompt", &json!({})).await.unwrap();
        }
        let tail = select_events_tail(&db, "t2", 10).await.unwrap();
        assert_eq!(tail.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn select_events_tail_empty_session() {
        let db = fresh_db().await;
        insert_session(&db, &row("t3")).await.unwrap();
        let tail = select_events_tail(&db, "t3", 5).await.unwrap();
        assert!(tail.is_empty());
    }

    #[tokio::test]
    async fn select_messages_tail_filters_to_transcript() {
        let db = fresh_db().await;
        insert_session(&db, &row("t4")).await.unwrap();
        append_event(&db, "t4", "t", "user_prompt", &json!({})).await.unwrap();        // 1
        append_event(&db, "t4", "t", "notification", &json!({})).await.unwrap();       // 2 (excluded)
        append_event(&db, "t4", "t", "assistant_message", &json!({})).await.unwrap();  // 3
        append_event(&db, "t4", "t", "permission_request", &json!({})).await.unwrap(); // 4 (excluded)
        append_event(&db, "t4", "t", "turn_end", &json!({})).await.unwrap();           // 5
        // Full tail: only the 3 transcript kinds, ascending.
        let tail = select_messages_tail(&db, "t4", 10).await.unwrap();
        assert_eq!(
            tail.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        // Small tail: the last two *transcript* rows, not the last two overall.
        let tail2 = select_messages_tail(&db, "t4", 2).await.unwrap();
        assert_eq!(tail2.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 5]);
    }
}
