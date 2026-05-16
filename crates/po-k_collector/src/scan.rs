//! Walk the projects root, register a watermark per file, read new events from each
//! file's byte_offset, return parsed [`Event`]s plus the new cursor state.
//!
//! Pure: no HTTP, no watermark persistence. The caller decides what to do with the
//! emitted events (ship them) and when to commit the new watermark (after the ship acks).

use anyhow::Result;
use po_k_core::{Event, MachineId, SessionKey};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tracing::warn;
use walkdir::WalkDir;

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
/// if the file is new). Detects rotation/truncation by comparing inode + head hash
/// and resets to byte 0 in that case.
pub async fn scan_file(
    root: &Path,
    file: &Path,
    store: &WatermarkStore,
    machine_id: &MachineId,
) -> Result<Option<ScanResult>> {
    let abs_path = file.to_string_lossy().to_string();
    let inode = inode_of(file)?;
    let head_hash = head_hash_of(file)?;
    let metadata = tokio::fs::metadata(file).await?;
    let file_size = metadata.len();

    let prior = store.get(&abs_path).await?;
    let (mut byte_offset, mut line_no) = match &prior {
        Some(wm) => {
            // Invalidate if either:
            //  - the inode changed (file replaced) — Unix only; on other platforms inode=0
            //    so we fall back to the head-hash check below.
            //  - the file shrank below our cursor (truncated, or new file at same path).
            //  - on platforms without inode, the head bytes changed.
            let inode_swapped = inode != 0 && wm.inode != 0 && wm.inode != inode;
            let truncated = file_size < wm.byte_offset;
            let head_changed = inode == 0 && wm.head_hash != head_hash;
            if inode_swapped || truncated || head_changed {
                warn!(file = %abs_path, "watermark invalidated (rotation/truncation detected), restarting from byte 0");
                (0u64, 0u64)
            } else {
                (wm.byte_offset, wm.line_no)
            }
        }
        None => (0, 0),
    };

    if file_size <= byte_offset {
        // Nothing new since last scan.
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

        // Only emit complete (newline-terminated) lines. A trailing partial line means
        // the file is mid-write; the next scan will see it complete.
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

        match Event::from_jsonl_line(
            line_bytes,
            session_key.clone(),
            relpath.clone(),
            line_start,
            line_no,
        ) {
            Ok(ev) => events.push(ev),
            Err(e) => warn!(file = %abs_path, line = line_no, error = %e, "skipping malformed jsonl line"),
        }
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
        },
    }))
}
