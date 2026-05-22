//! Read a Claude Code transcript JSONL file from a saved byte offset and group
//! the resulting events into "turns" delimited by `last-prompt.leafUuid`.

use anyhow::{Context, Result};
use std::io::Read;
use std::path::Path;

use crate::text;

#[derive(Debug, Clone, Default)]
pub struct Turn {
    /// `last-prompt.leafUuid`. Empty when no prompt boundary was seen yet.
    pub turn_id: String,
    /// All raw JSONL lines in this turn (in source order).
    pub raw_lines: Vec<String>,
    /// Concatenated human-readable text of every event we know how to render.
    pub searchable: String,
}

impl Turn {
    fn push(&mut self, raw: String) {
        let extracted = text::extract_searchable(&raw);
        if !extracted.trim().is_empty() {
            if !self.searchable.is_empty() {
                self.searchable.push_str("\n\n");
            }
            self.searchable.push_str(&extracted);
        }
        self.raw_lines.push(raw);
    }
}

#[derive(Debug, Clone)]
pub struct TailResult {
    /// Turns assembled from the new slice. The last turn may still be open.
    pub turns: Vec<Turn>,
    /// New byte offset to persist as the watermark for this file.
    pub new_offset: u64,
}

/// Read the new portion of `transcript_path` starting at `from_offset`, group
/// the events into turns, and return the result + the offset to remember.
pub fn tail(transcript_path: &Path, from_offset: u64) -> Result<TailResult> {
    let mut f = std::fs::File::open(transcript_path)
        .with_context(|| format!("opening {}", transcript_path.display()))?;
    let len = f.metadata()?.len();
    if from_offset >= len {
        return Ok(TailResult {
            turns: Vec::new(),
            new_offset: from_offset,
        });
    }
    use std::io::Seek;
    f.seek(std::io::SeekFrom::Start(from_offset))?;
    let mut buf = Vec::with_capacity((len - from_offset) as usize);
    f.read_to_end(&mut buf)?;
    // If the file was being written we may have a partial trailing line; drop
    // anything after the last \n and advance the offset accordingly.
    let last_nl = match buf.iter().rposition(|b| *b == b'\n') {
        Some(p) => p + 1,
        None => 0,
    };
    let new_offset = from_offset + last_nl as u64;
    let text = std::str::from_utf8(&buf[..last_nl]).unwrap_or("");

    let mut turns: Vec<Turn> = Vec::new();
    let mut current = Turn::default();

    for line in text.split('\n') {
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let kind = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
        if kind == "last-prompt" {
            // Close the current turn (if non-empty) and start a fresh one.
            if !current.raw_lines.is_empty() {
                turns.push(std::mem::take(&mut current));
            }
            current.turn_id = v
                .get("leafUuid")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string();
            continue;
        }
        current.push(line.to_string());
    }
    if !current.raw_lines.is_empty() {
        turns.push(current);
    }
    Ok(TailResult { turns, new_offset })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn tails_and_groups_by_last_prompt() {
        let tmp = std::env::temp_dir().join(format!("po-k-tail-{}.jsonl", std::process::id()));
        let mut f = std::fs::File::create(&tmp).unwrap();
        writeln!(f, "{{\"type\":\"last-prompt\",\"leafUuid\":\"t1\"}}").unwrap();
        writeln!(
            f,
            "{{\"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"hello\"}}}}"
        )
        .unwrap();
        writeln!(
            f,
            "{{\"type\":\"assistant\",\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"hi back\"}}]}}}}"
        ).unwrap();
        writeln!(f, "{{\"type\":\"last-prompt\",\"leafUuid\":\"t2\"}}").unwrap();
        writeln!(
            f,
            "{{\"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"more\"}}}}"
        )
        .unwrap();
        f.flush().unwrap();

        let r = tail(&tmp, 0).unwrap();
        assert_eq!(r.turns.len(), 2);
        assert_eq!(r.turns[0].turn_id, "t1");
        assert_eq!(r.turns[1].turn_id, "t2");
        assert!(r.turns[0].searchable.contains("hello"));
        assert!(r.turns[0].searchable.contains("hi back"));
        let _ = std::fs::remove_file(&tmp);
    }
}
