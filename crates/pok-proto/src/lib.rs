//! Shared wire protocol between po-k (WebSocket client) and Xpo-k (WebSocket
//! server / sole HTTP entry point). All frames are JSON text; the message type
//! is tagged on `"type"` (spec §4.4). Request/response pairs correlate by
//! `request_id` (UUID); many can be in flight at once on one socket.

pub mod profile;

pub use profile::Profile;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// A project a po-k instance owns, declared at registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDecl {
    pub name: String,
    pub cwd: String,
}

/// Capability flags a po-k instance advertises at registration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PokCaps {
    /// When true, this po-k accepts sessions in arbitrary directories (no
    /// pre-configured project required).
    #[serde(default)]
    pub ad_hoc: bool,
}

/// An active session a po-k instance is tracking, declared at registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDecl {
    pub sid: String,
    pub project: String,
    #[serde(default)]
    pub status: String,
}

/// A forwarded session event.
///
/// `seq`/`ts` are additive (M15): they carry po-k's per-session monotonic
/// sequence number so Xpo-k can order, deduplicate, and resume from forwarded
/// events. Both default, so frames from an older po-k still deserialize —
/// `seq == 0` means "unsequenced" and is treated as non-resumable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub seq: i64,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    NotFound,
    Conflict,
    Internal,
    Timeout,
    Disconnected,
    BadRequest,
}

/// Every message that can cross the po-k ↔ Xpo-k WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMsg {
    // ---- po-k → Xpo-k ----
    Register {
        pok_id: String,
        hostname: String,
        version: String,
        #[serde(default)]
        projects: Vec<ProjectDecl>,
        #[serde(default)]
        sessions: Vec<SessionDecl>,
        /// Capability flags (ad_hoc support, etc.). Defaults for old po-k builds.
        #[serde(default)]
        caps: PokCaps,
    },
    ConfigUpdate {
        projects: Vec<ProjectDecl>,
    },
    ProfileAck {
        request_id: Uuid,
        plugin_dir: String,
    },
    WsResponse {
        request_id: Uuid,
        status: u16,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        body: String,
    },
    WsStreamChunk {
        request_id: Uuid,
        data: String,
    },
    WsStreamEnd {
        request_id: Uuid,
    },
    SessionEvent {
        sid: String,
        event: EventEnvelope,
    },
    StatusUpdate {
        sid: String,
        status: String,
    },

    // ---- Xpo-k → po-k ----
    Registered {
        pok_id: String,
    },
    PushProfile {
        request_id: Uuid,
        #[serde(default)]
        session_id: Option<String>,
        profile: serde_json::Value,
    },
    ProfileUpdate {
        session_id: String,
        profile: serde_json::Value,
        #[serde(default)]
        changed_fields: Vec<String>,
    },
    WsRequest {
        request_id: Uuid,
        method: String,
        path: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        body: Option<String>,
        #[serde(default)]
        stream: bool,
    },
    WsCancel {
        request_id: Uuid,
    },

    // ---- either direction ----
    Error {
        #[serde(default)]
        request_id: Option<Uuid>,
        code: ErrorCode,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(m: &WsMsg) -> serde_json::Value {
        let s = serde_json::to_string(m).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        // also confirm it deserializes back into WsMsg
        let _back: WsMsg = serde_json::from_str(&s).unwrap();
        v
    }

    #[test]
    fn wire_type_tags() {
        let v = roundtrip(&WsMsg::Register {
            pok_id: "p".into(),
            hostname: "h".into(),
            version: "0".into(),
            projects: vec![],
            sessions: vec![],
            caps: PokCaps::default(),
        });
        assert_eq!(v["type"], "register");

        let v = roundtrip(&WsMsg::WsRequest {
            request_id: Uuid::nil(),
            method: "GET".into(),
            path: "/health".into(),
            headers: Default::default(),
            body: None,
            stream: false,
        });
        assert_eq!(v["type"], "ws_request");
        assert_eq!(v["stream"], false);

        let v = roundtrip(&WsMsg::Error {
            request_id: None,
            code: ErrorCode::NotFound,
            message: "x".into(),
        });
        assert_eq!(v["type"], "error");
        assert_eq!(v["code"], "not_found");
    }

    #[test]
    fn session_event_carries_seq_and_ts() {
        let v = roundtrip(&WsMsg::SessionEvent {
            sid: "s1".into(),
            event: EventEnvelope {
                kind: "stop".into(),
                payload: serde_json::json!({"last_assistant_message": "done"}),
                seq: 12,
                ts: "2026-08-03T10:00:00Z".into(),
            },
        });
        assert_eq!(v["type"], "session_event");
        assert_eq!(v["event"]["seq"], 12);
        assert_eq!(v["event"]["ts"], "2026-08-03T10:00:00Z");
    }

    #[test]
    fn session_event_from_an_older_pok_still_decodes() {
        // Pre-M15 po-k builds send no seq/ts. Both default, so the frame stays
        // decodable — Xpo-k treats seq 0 as "unsequenced" (not resumable) rather
        // than failing the connection.
        let old = r#"{"type":"session_event","sid":"s1","event":{"kind":"stop"}}"#;
        let msg: WsMsg = serde_json::from_str(old).unwrap();
        match msg {
            WsMsg::SessionEvent { sid, event } => {
                assert_eq!(sid, "s1");
                assert_eq!(event.kind, "stop");
                assert_eq!(event.seq, 0);
                assert_eq!(event.ts, "");
                assert!(event.payload.is_null());
            }
            other => panic!("expected session_event, got {other:?}"),
        }
    }
}
