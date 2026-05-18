use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::SessionKey;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid utf-8 in jsonl line")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
}

/// One ingested record. The parsed projection is what the server queries on;
/// `raw` keeps the original line verbatim so a schema drift never silently drops data.
///
/// JSONL is UTF-8 by spec so `raw` is a `String`; if Claude Code ever writes non-UTF-8
/// we'd reject earlier in [`Event::from_jsonl_line`] and we'd want to know.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub session_key: SessionKey,

    /// Relative to `~/.claude/projects/`. e.g. `-workspace/<uuid>.jsonl`
    /// or `-workspace/<uuid>/subagents/agent-<id>.jsonl`.
    pub file_relpath: String,

    /// Byte offset of the start of this line in the source file.
    pub byte_offset: u64,

    /// 0-indexed line number within the source file.
    pub line_no: u64,

    /// ISO-8601 string as written by Claude Code; we keep it verbatim.
    #[serde(default)]
    pub timestamp: Option<String>,

    /// The `type` field from the original event. Empty when the event omitted it.
    /// We keep this as a string instead of an enum so a new event type Anthropic
    /// ships still round-trips with full fidelity.
    #[serde(default)]
    pub kind: String,

    /// Indicates the event belongs to a subagent's transcript file.
    #[serde(default)]
    pub is_sidechain: bool,

    /// For subagent events, the agent id surfaced in the subagent jsonl rows.
    /// Empty when this is a main-session event.
    #[serde(default)]
    pub agent_id: String,

    /// `last-prompt.leafUuid` of the most recent prompt boundary at the time this
    /// event was read, threaded by the collector so every event in a turn shares
    /// the same id. Empty when no prompt boundary has been observed yet.
    #[serde(default)]
    pub turn_id: String,

    /// Original (unsanitized) `cwd` from the event when it carried one. `last-prompt`
    /// / `permission-mode` events don't, so this is best-effort and inherits the
    /// session's last seen cwd at the collector when missing.
    #[serde(default)]
    pub original_cwd: String,

    /// Project slug resolved by the collector from `original_cwd` against its local
    /// projects.toml. Empty when no rule matched — the server falls back to its
    /// project_aliases table and ultimately to "unassigned".
    #[serde(default)]
    pub project_id: String,

    /// The original JSONL line, sans trailing newline.
    pub raw: String,
}

/// Well-known kind strings, for clarity at the call sites. The string-typed `kind`
/// field is still the source of truth.
pub mod kind {
    pub const USER: &str = "user";
    pub const ASSISTANT: &str = "assistant";
    pub const SYSTEM: &str = "system";
    pub const ATTACHMENT: &str = "attachment";
    pub const LAST_PROMPT: &str = "last-prompt";
    pub const PERMISSION_MODE: &str = "permission-mode";
    pub const AI_TITLE: &str = "ai-title";
    pub const FILE_HISTORY_SNAPSHOT: &str = "file-history-snapshot";
}

impl Event {
    /// Parse a single JSONL line into an `Event`. The `raw` string is stored even if the
    /// projection extraction omits fields, so the server always has the full original line.
    pub fn from_jsonl_line(
        line: &[u8],
        session_key: SessionKey,
        file_relpath: impl Into<String>,
        byte_offset: u64,
        line_no: u64,
    ) -> Result<Self, ParseError> {
        let text = std::str::from_utf8(line)?;
        let v: Value = serde_json::from_str(text)?;

        let kind = v
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let timestamp = v
            .get("timestamp")
            .and_then(Value::as_str)
            .map(str::to_string);
        let is_sidechain = v
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let agent_id = v
            .get("agentId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        Ok(Self {
            session_key,
            file_relpath: file_relpath.into(),
            byte_offset,
            line_no,
            timestamp,
            kind,
            is_sidechain,
            agent_id,
            turn_id: String::new(),
            original_cwd: String::new(),
            project_id: String::new(),
            raw: text.to_string(),
        })
    }

    /// Pull the top-level `cwd` field out of the parsed raw line if present. Best-effort.
    pub fn extract_cwd(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_str(&self.raw).ok()?;
        v.get("cwd")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }

    /// Pull `leafUuid` from a `last-prompt` event. Used by the collector to seed
    /// the running turn_id for the file.
    pub fn extract_last_prompt_leaf(&self) -> Option<String> {
        if self.kind != kind::LAST_PROMPT {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&self.raw).ok()?;
        v.get("leafUuid")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MachineId;

    fn key() -> SessionKey {
        SessionKey::derive(
            &MachineId::from("test-machine"),
            "-workspace",
            "00000000-0000-0000-0000-000000000000",
        )
    }

    #[test]
    fn parses_assistant_event() {
        let line = br#"{"type":"assistant","uuid":"abc","timestamp":"2026-05-16T09:51:19.994Z","isSidechain":false,"message":{"role":"assistant","content":[]}}"#;
        let ev = Event::from_jsonl_line(line, key(), "-workspace/x.jsonl", 0, 0).unwrap();
        assert_eq!(ev.kind, "assistant");
        assert_eq!(ev.timestamp.as_deref(), Some("2026-05-16T09:51:19.994Z"));
        assert!(!ev.is_sidechain);
        assert_eq!(ev.raw.as_bytes(), line);
    }

    #[test]
    fn unknown_type_survives_as_string() {
        let line = br#"{"type":"future-event-shape","note":"hi"}"#;
        let ev = Event::from_jsonl_line(line, key(), "x", 0, 0).unwrap();
        assert_eq!(ev.kind, "future-event-shape");
    }

    #[test]
    fn missing_type_keeps_event() {
        let line = br#"{"note":"only-note"}"#;
        let ev = Event::from_jsonl_line(line, key(), "x", 0, 0).unwrap();
        assert_eq!(ev.kind, "");
        assert_eq!(ev.raw.as_bytes(), line);
    }

    #[test]
    fn raw_is_preserved_with_offsets() {
        let line = br#"{"type":"system","subtype":"informational","content":"hi"}"#;
        let ev = Event::from_jsonl_line(line, key(), "x", 10, 3).unwrap();
        assert_eq!(ev.raw.as_bytes(), line);
        assert_eq!(ev.byte_offset, 10);
        assert_eq!(ev.line_no, 3);
    }

    #[test]
    fn detects_subagent_fields() {
        let line = br#"{"type":"assistant","isSidechain":true,"agentId":"a88bc97aadc91deee"}"#;
        let ev = Event::from_jsonl_line(line, key(), "x", 0, 0).unwrap();
        assert!(ev.is_sidechain);
        assert_eq!(ev.agent_id, "a88bc97aadc91deee");
    }
}
