//! Per-file cursor persistence so the collector resumes where it left off.
//!
//! Stored in a small SQLite under `~/.config/po-k/collector.db`. Keyed by absolute path.
//! We additionally fingerprint the file's first 256 bytes so that if Claude Code ever
//! truncates / replaces a session file the watermark resets to 0 instead of skipping
//! ahead into the new file's content.

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Watermark {
    pub abs_path: String,
    pub inode: i64,
    pub head_hash: String,
    pub byte_offset: u64,
    pub line_no: u64,
    /// Most recent `last-prompt.leafUuid` observed in this file, threaded through
    /// across scan_file invocations so events after a resume still get the right
    /// turn_id.
    pub last_turn_id: String,
}

#[derive(Clone)]
pub struct WatermarkStore {
    pool: SqlitePool,
}

impl WatermarkStore {
    pub async fn open(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let opts = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS watermarks (
                abs_path      TEXT PRIMARY KEY,
                inode         INTEGER NOT NULL,
                head_hash     TEXT NOT NULL,
                byte_offset   INTEGER NOT NULL,
                line_no       INTEGER NOT NULL,
                last_turn_id  TEXT NOT NULL DEFAULT '',
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )
        .execute(&pool)
        .await?;
        // Schema extension for collectors that already have a watermarks table from a
        // previous version — additive, idempotent.
        let _ = sqlx::query("ALTER TABLE watermarks ADD COLUMN last_turn_id TEXT NOT NULL DEFAULT ''")
            .execute(&pool)
            .await;
        Ok(Self { pool })
    }

    pub async fn get(&self, abs_path: &str) -> Result<Option<Watermark>> {
        let row = sqlx::query_as::<_, (String, i64, String, i64, i64, String)>(
            "SELECT abs_path, inode, head_hash, byte_offset, line_no, last_turn_id
             FROM watermarks WHERE abs_path = ?",
        )
        .bind(abs_path)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(abs_path, inode, head_hash, off, ln, turn)| Watermark {
            abs_path,
            inode,
            head_hash,
            byte_offset: off as u64,
            line_no: ln as u64,
            last_turn_id: turn,
        }))
    }

    pub async fn upsert(&self, wm: &Watermark) -> Result<()> {
        sqlx::query(
            "INSERT INTO watermarks (abs_path, inode, head_hash, byte_offset, line_no, last_turn_id)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(abs_path) DO UPDATE SET
                inode = excluded.inode,
                head_hash = excluded.head_hash,
                byte_offset = excluded.byte_offset,
                line_no = excluded.line_no,
                last_turn_id = excluded.last_turn_id,
                updated_at = datetime('now')",
        )
        .bind(&wm.abs_path)
        .bind(wm.inode)
        .bind(&wm.head_hash)
        .bind(wm.byte_offset as i64)
        .bind(wm.line_no as i64)
        .bind(&wm.last_turn_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub fn default_db_path() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("po-k").join("collector.db")
}

/// Fingerprint the first 256 bytes of the file to detect rotation / replacement.
/// Tiny so we re-read on every poll without measurable cost.
pub fn head_hash_of(path: &Path) -> Result<String> {
    let mut buf = [0u8; 256];
    let n = read_head_bytes(path, &mut buf)?;
    Ok(blake3::hash(&buf[..n]).to_hex().to_string())
}

fn read_head_bytes(path: &Path, buf: &mut [u8]) -> Result<usize> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut total = 0;
    while total < buf.len() {
        match f.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

#[cfg(unix)]
pub fn inode_of(path: &Path) -> Result<i64> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    Ok(m.ino() as i64)
}

#[cfg(not(unix))]
pub fn inode_of(_path: &Path) -> Result<i64> {
    Ok(0)
}
