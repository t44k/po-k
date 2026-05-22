//! Daemon-side state file. Tiny JSON map persisted at the config's `state_db` path
//! (yes, the field is named `state_db` for historical reasons — for now it's a
//! JSON file). Atomic write via write-to-tmp + rename.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// When the current daemon started (ISO 8601 / RFC 3339).
    pub started_at: Option<String>,
    /// PID of the running daemon, when this file is held open by one.
    pub pid: Option<u32>,
    /// Per-repo metadata, keyed by absolute repo path.
    pub repos: BTreeMap<PathBuf, RepoState>,
    /// Per-jsonl watermark (used by M10.4 for the transcript tail).
    pub jsonl: BTreeMap<PathBuf, u64>,
    /// Per-topic last distillation time.
    pub topics: BTreeMap<String, TopicState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoState {
    pub last_pull_at: Option<String>,
    pub last_pull_ok: bool,
    pub last_push_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicState {
    pub last_distill_at: Option<String>,
    pub last_evidence_count: u32,
}

pub fn load(path: &Path) -> Result<State> {
    if !path.exists() {
        return Ok(State::default());
    }
    let bytes = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let s: State = serde_json::from_str(&bytes)
        .with_context(|| format!("parsing {}", path.display()))?;
    Ok(s)
}

pub fn save(path: &Path, s: &State) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(s)?;
    std::fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming {} → {}", tmp.display(), path.display()))?;
    Ok(())
}

pub fn now_iso() -> String {
    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    // Crude RFC 3339 in UTC without chrono — sufficient for state file timestamps.
    let secs = t.as_secs() as i64;
    let (year, month, day, h, m, s) = epoch_to_ymdhms(secs);
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Inverse of the cumulative-days table. Good enough for "now" timestamps; not for
/// distant dates.
fn epoch_to_ymdhms(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400) as i64;
    let mut remaining_secs = secs.rem_euclid(86_400) as u32;
    let h = remaining_secs / 3600;
    remaining_secs %= 3600;
    let m = remaining_secs / 60;
    let s = remaining_secs % 60;

    // 1970-01-01 was a Thursday; we just need a date back-projector.
    let mut year: i32 = 1970;
    let mut days = days;
    loop {
        let yd = if is_leap(year) { 366 } else { 365 };
        if days < yd {
            break;
        }
        days -= yd;
        year += 1;
    }
    let month_days: [u32; 12] = if is_leap(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u32;
    let mut days = days as u32;
    for &d in &month_days {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = days + 1;
    (year, month, day, h, m, s)
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_iso_shape() {
        let s = now_iso();
        // YYYY-MM-DDTHH:MM:SSZ = 20 chars
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
    }

    #[test]
    fn roundtrip_state() {
        let tmp = std::env::temp_dir().join(format!("po-k-state-test-{}.json", std::process::id()));
        let mut s = State::default();
        s.started_at = Some(now_iso());
        s.repos.insert(
            PathBuf::from("/tmp/repo"),
            RepoState {
                last_pull_at: Some(now_iso()),
                last_pull_ok: true,
                last_push_at: None,
            },
        );
        save(&tmp, &s).unwrap();
        let loaded = load(&tmp).unwrap();
        assert_eq!(loaded.repos.len(), 1);
        let _ = std::fs::remove_file(&tmp);
    }
}
