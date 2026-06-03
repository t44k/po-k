//! SQLite persistence for Xpo-k (spec §4.5): profiles + version history, plus
//! the (rebuilt-on-restart) registry tables for connected po-k instances.

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Db = Pool<Sqlite>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS profiles (
    name        TEXT PRIMARY KEY,
    version     TEXT NOT NULL DEFAULT '1.0.0',
    description TEXT,
    tags        TEXT,
    data        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profile_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    version     TEXT NOT NULL,
    data        TEXT NOT NULL,
    changed_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS xpok_sessions (
    sid         TEXT PRIMARY KEY,
    pok_id      TEXT NOT NULL,
    project     TEXT NOT NULL,
    profiles    TEXT,
    status      TEXT NOT NULL DEFAULT 'idle',
    started_at  TEXT NOT NULL,
    ended_at    TEXT
);
"#;

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

pub fn now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Reuse a tiny formatter to avoid a chrono dependency.
    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as i64;
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z / 146_097 } else { (z - 146_096) / 146_097 };
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

#[derive(Debug, Clone)]
pub struct ProfileRow {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub tags: Option<String>,
    pub data: String,
    pub created_at: String,
    pub updated_at: String,
}

/// (name, version, description, tags, data, created_at, updated_at)
type ProfileTuple = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);

fn profile_row_from(t: ProfileTuple) -> ProfileRow {
    let (name, version, description, tags, data, created_at, updated_at) = t;
    ProfileRow {
        name,
        version,
        description,
        tags,
        data,
        created_at,
        updated_at,
    }
}

impl ProfileRow {
    /// Summary JSON for `GET /profiles` listing.
    pub fn summary(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "tags": self.tags.as_deref().and_then(|t| serde_json::from_str::<Value>(t).ok()).unwrap_or(Value::Array(vec![])),
        })
    }
}

pub async fn list_profiles(db: &Db) -> Result<Vec<ProfileRow>> {
    let rows: Vec<ProfileTuple> = sqlx::query_as(
        "SELECT name, version, description, tags, data, created_at, updated_at FROM profiles ORDER BY name",
    )
    .fetch_all(db)
    .await
    .context("SELECT profiles")?;
    Ok(rows.into_iter().map(profile_row_from).collect())
}

pub async fn get_profile(db: &Db, name: &str) -> Result<Option<ProfileRow>> {
    let row: Option<ProfileTuple> = sqlx::query_as(
        "SELECT name, version, description, tags, data, created_at, updated_at FROM profiles WHERE name = ?1",
    )
    .bind(name)
    .fetch_optional(db)
    .await
    .context("SELECT profile")?;
    Ok(row.map(profile_row_from))
}

/// Upsert a profile, recording the prior+new data in `profile_history`.
pub async fn upsert_profile(db: &Db, profile: &Value) -> Result<ProfileRow> {
    let name = profile
        .get("name")
        .and_then(|v| v.as_str())
        .context("profile missing name")?
        .to_string();
    let version = profile
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("1.0.0")
        .to_string();
    let description = profile
        .get("description")
        .and_then(|v| v.as_str())
        .map(String::from);
    let tags = profile.get("tags").map(|t| t.to_string());
    let data = serde_json::to_string(profile)?;
    let now = now_iso();

    let existing = get_profile(db, &name).await?;
    let created_at = existing
        .as_ref()
        .map(|r| r.created_at.clone())
        .unwrap_or_else(|| now.clone());

    sqlx::query(
        r#"INSERT INTO profiles (name, version, description, tags, data, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
           ON CONFLICT(name) DO UPDATE SET
             version = ?2, description = ?3, tags = ?4, data = ?5, updated_at = ?7"#,
    )
    .bind(&name)
    .bind(&version)
    .bind(&description)
    .bind(&tags)
    .bind(&data)
    .bind(&created_at)
    .bind(&now)
    .execute(db)
    .await
    .context("UPSERT profile")?;

    sqlx::query(
        "INSERT INTO profile_history (name, version, data, changed_at) VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(&name)
    .bind(&version)
    .bind(&data)
    .bind(&now)
    .execute(db)
    .await
    .context("INSERT profile_history")?;

    Ok(ProfileRow {
        name,
        version,
        description,
        tags,
        data,
        created_at,
        updated_at: now,
    })
}

pub async fn delete_profile(db: &Db, name: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM profiles WHERE name = ?1")
        .bind(name)
        .execute(db)
        .await
        .context("DELETE profile")?;
    Ok(res.rows_affected() > 0)
}

pub async fn profile_history(db: &Db, name: &str) -> Result<Vec<Value>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT version, data, changed_at FROM profile_history WHERE name = ?1 ORDER BY id DESC",
    )
    .bind(name)
    .fetch_all(db)
    .await
    .context("SELECT profile_history")?;
    Ok(rows
        .into_iter()
        .map(|(version, _data, changed_at)| serde_json::json!({ "version": version, "changed_at": changed_at }))
        .collect())
}

/// Live (not-ended) sessions: `(sid, pok_id, profile_names)`. Used by Phase 4
/// to find sessions affected by a profile change.
pub async fn live_sessions(db: &Db) -> Result<Vec<(String, String, Vec<String>)>> {
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT sid, pok_id, profiles FROM xpok_sessions WHERE ended_at IS NULL",
    )
    .fetch_all(db)
    .await
    .context("SELECT live xpok_sessions")?;
    Ok(rows
        .into_iter()
        .map(|(sid, pok_id, profiles)| {
            let names = profiles
                .as_deref()
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default();
            (sid, pok_id, names)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn fresh() -> Db {
        let p = std::env::temp_dir().join(format!("xpok-test-{}.db", uuid::Uuid::new_v4()));
        open(&p).await.unwrap()
    }

    #[tokio::test]
    async fn crud_and_history() {
        let db = fresh().await;
        let p = json!({ "name": "base", "version": "1.0.0", "description": "d", "tags": ["x"] });
        upsert_profile(&db, &p).await.unwrap();
        let got = get_profile(&db, "base").await.unwrap().unwrap();
        assert_eq!(got.version, "1.0.0");

        // Update bumps history.
        let p2 = json!({ "name": "base", "version": "1.1.0" });
        upsert_profile(&db, &p2).await.unwrap();
        let hist = profile_history(&db, "base").await.unwrap();
        assert_eq!(hist.len(), 2);
        assert_eq!(get_profile(&db, "base").await.unwrap().unwrap().version, "1.1.0");

        assert!(delete_profile(&db, "base").await.unwrap());
        assert!(get_profile(&db, "base").await.unwrap().is_none());
    }
}
