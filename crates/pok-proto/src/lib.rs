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

/// An active session a po-k instance is tracking, declared at registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDecl {
    pub sid: String,
    pub project: String,
    #[serde(default)]
    pub status: String,
}

/// A forwarded session event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
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
}
