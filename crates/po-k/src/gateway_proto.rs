//! Wire types for the `po-k gateway` JSONL stdio bridge.
//!
//! Each frame is one JSON object per line, `\n`-terminated.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Inbound: remote → po-k.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound {
    /// Push `text` into the named project's zellij pane.
    Prompt {
        project: String,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachments: Option<Vec<Attachment>>,
    },
    /// Verbs: `interrupt` (ESC), `clear` (/clear), `submit` (\n).
    Command {
        project: String,
        verb: String,
    },
    /// Synchronous request/reply by `method`. v1 supports
    /// `projects.list` and `memory.recall`.
    Query {
        method: String,
        #[serde(default)]
        params: Value,
        id: String,
    },
    /// Heartbeat — replied with `pong`.
    Ping {
        #[serde(default)]
        ts: Option<String>,
    },
}

/// Outbound: po-k → remote.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Hello {
        version: &'static str,
        repo: HelloRepo,
    },
    Result {
        id: String,
        ok: bool,
        value: Value,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        message: String,
    },
    Event {
        #[serde(skip_serializing_if = "Option::is_none")]
        project: Option<String>,
        kind: String,
        #[serde(flatten)]
        payload: Value,
    },
    Pong {
        ts: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct HelloRepo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_pull: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub media_type: String,
    #[serde(default)]
    pub encoding: String,
    pub data: String,
}
