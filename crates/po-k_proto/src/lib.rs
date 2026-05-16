//! Wire format for collector → server.
//!
//! Body of `POST /ingest` is NDJSON: one [`BatchHeader`] line followed by N event lines
//! (each one a [`po_k_core::Event`] serialized as JSON). Two reasons we keep it NDJSON
//! and not a single JSON object: (a) the server can parse line-by-line and reject
//! individual malformed events without losing the whole batch, (b) it matches the
//! source on disk and keeps the round-trip simple.

use po_k_core::MachineId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchHeader {
    /// Required marker so the server knows the first line is the header.
    #[serde(rename = "type")]
    pub kind: BatchKind,
    pub batch_id: String,
    pub machine_id: MachineId,
    pub sent_at: String,
    pub count: u64,
    /// Optional team binding hint. M1 ignores this and uses `default`.
    #[serde(default)]
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BatchKind {
    BatchHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum IngestResponse {
    Ok {
        accepted: u64,
        duplicates: u64,
    },
    Error {
        message: String,
        #[serde(default)]
        rejected_line: Option<u64>,
    },
}

pub const HEADER_API_KEY: &str = "x-api-key";
pub const HEADER_IDEMPOTENCY_KEY: &str = "idempotency-key";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let h = BatchHeader {
            kind: BatchKind::BatchHeader,
            batch_id: "01J0".into(),
            machine_id: MachineId::from("m1"),
            sent_at: "2026-05-16T09:51:19Z".into(),
            count: 42,
            team_id: None,
        };
        let s = serde_json::to_string(&h).unwrap();
        let back: BatchHeader = serde_json::from_str(&s).unwrap();
        assert_eq!(back.batch_id, "01J0");
        assert_eq!(back.count, 42);
        assert_eq!(back.kind, BatchKind::BatchHeader);
    }
}

// keep `Event` reachable through the proto crate so the collector / server pull
// the canonical type from one place
pub use po_k_core::Event as ProtoEvent;
