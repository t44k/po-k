//! Walk the projects root, register a watermark per file, read new events from each
//! file's byte_offset, return parsed [`Event`]s plus the new cursor state.
//!
//! Pure: no HTTP, no watermark persistence. The caller decides what to do with the
//! emitted events (ship them) and when to commit the new watermark (after the ship acks).

use anyhow::Result;
use po_k_core::{kind, Event, MachineId, SessionKey};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tracing::warn;
use walkdir::WalkDir;

use crate::projects::ProjectMap;
use crate::watermark::{head_hash_of, inode_of, Watermark, WatermarkStore};

/// All the events read from a single file in one pass, plus the updated watermark
/// to persist if shipping succeeds.
pub struct ScanResult {
    pub events: Vec<Event>,
    pub next_watermark: Watermark,
}

pub fn discover_jsonl_files(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|p| p.extension().map(|x| x == "jsonl").unwrap_or(false))
        .collect();
    out.sort();
    out
}

/// Map a file path to `(sanitized_cwd, session_uuid)`. None if the path doesn't have
/// the expected shape under `root`.
pub fn identify_session(root: &Path, file: &Path) -> Option<(String, String)> {
    let rel = file.strip_prefix(root).ok()?;
    let mut comps = rel.components();
    let sanitized_cwd = comps.next()?.as_os_str().to_string_lossy().to_string();
    let second = comps.next()?.as_os_str().to_string_lossy().to_string();
    let session_uuid = second.strip_suffix(".jsonl").unwrap_or(&second).to_string();
    Some((sanitized_cwd, session_uuid))
}

/// Read all new events from `file`, starting at the watermark's byte_offset (or 0
/// if the file is new). Stamps each event with `original_cwd`, `project_id`
/// (resolved against `projects`), and the running `turn_id` (threaded through
/// `last-prompt` events).
pub async fn scan_file(
    root: &Path,
    file: &Path,
    store: &WatermarkStore,
    machine_id: &MachineId,
    projects: &ProjectMap,
) -> Result<Option<ScanResult>> {
    let abs_path = file.to_string_lossy().to_string();
    let inode = inode_of(file)?;
    let head_hash = head_hash_of(file)?;
    let metadata = tokio::fs::metadata(file).await?;
    let file_size = metadata.len();

    let prior = store.get(&abs_path).await?;
    let (mut byte_offset, mut line_no, mut current_turn_id, mut current_cwd) = match &prior {
        Some(wm) => {
            let inode_swapped = inode != 0 && wm.inode != 0 && wm.inode != inode;
            let truncated = file_size < wm.byte_offset;
            let head_changed = inode == 0 && wm.head_hash != head_hash;
            if inode_swapped || truncated || head_changed {
                warn!(file = %abs_path, "watermark invalidated (rotation/truncation detected), restarting from byte 0");
                (0u64, 0u64, String::new(), String::new())
            } else {
                (wm.byte_offset, wm.line_no, wm.last_turn_id.clone(), String::new())
            }
        }
        None => (0, 0, String::new(), String::new()),
    };

    if file_size <= byte_offset {
        return Ok(None);
    }

    let Some((sanitized_cwd, session_uuid)) = identify_session(root, file) else {
        warn!(file = %abs_path, "could not derive session identity, skipping");
        return Ok(None);
    };
    let session_key = SessionKey::derive(machine_id, &sanitized_cwd, &session_uuid);
    let relpath = match file.strip_prefix(root) {
        Ok(r) => r.to_string_lossy().to_string(),
        Err(_) => abs_path.clone(),
    };

    let mut f = tokio::fs::File::open(file).await?;
    f.seek(std::io::SeekFrom::Start(byte_offset)).await?;
    let mut reader = BufReader::new(f);

    let mut events: Vec<Event> = Vec::new();
    let mut buf = Vec::with_capacity(8 * 1024);
    loop {
        buf.clear();
        let read = reader.read_until(b'\n', &mut buf).await?;
        if read == 0 {
            break;
        }
        let line_start = byte_offset;

        let line_bytes: &[u8] = if buf.ends_with(b"\n") {
            &buf[..buf.len() - 1]
        } else {
            break;
        };
        byte_offset += read as u64;

        if line_bytes.is_empty() {
            line_no += 1;
            continue;
        }

        let mut ev = match Event::from_jsonl_line(
            line_bytes,
            session_key.clone(),
            relpath.clone(),
            line_start,
            line_no,
        ) {
            Ok(ev) => ev,
            Err(e) => {
                warn!(file = %abs_path, line = line_no, error = %e, "skipping malformed jsonl line");
                line_no += 1;
                continue;
            }
        };

        // If this event introduces a new prompt boundary, advance our running turn.
        if ev.kind == kind::LAST_PROMPT {
            if let Some(leaf) = ev.extract_last_prompt_leaf() {
                current_turn_id = leaf;
            }
        }
        ev.turn_id = current_turn_id.clone();

        // Stamp the cwd from the event when present; otherwise inherit the file's
        // last seen cwd. Then route the cwd through the project map.
        if let Some(cwd) = ev.extract_cwd() {
            if !cwd.is_empty() {
                current_cwd = cwd;
            }
        }
        ev.original_cwd = current_cwd.clone();
        if let Some(pid) = projects.resolve(&current_cwd) {
            ev.project_id = pid.to_string();
        }

        events.push(ev);
        line_no += 1;
    }

    Ok(Some(ScanResult {
        events,
        next_watermark: Watermark {
            abs_path,
            inode,
            head_hash,
            byte_offset,
            line_no,
            last_turn_id: current_turn_id,
        },
    }))
}
