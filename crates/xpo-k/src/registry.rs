//! In-memory registry of connected po-k instances + the correlation maps that
//! let an HTTP handler await a WebSocket round-trip. Rebuilt from `register`
//! messages on (re)connect — nothing here survives an Xpo-k restart.

use dashmap::DashMap;
use pok_proto::{ProjectDecl, WsMsg};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub type PokId = String;

/// A unary response coming back over the WebSocket.
#[derive(Debug, Clone)]
pub struct WsResult {
    pub status: u16,
    pub body: String,
}

/// A streaming frame for SSE/long-poll bridging.
#[derive(Debug, Clone)]
pub enum StreamFrame {
    Chunk(String),
    End,
    Error(String),
}

#[derive(Clone)]
pub struct PokConn {
    pub pok_id: PokId,
    pub hostname: String,
    pub version: String,
    pub tx: mpsc::UnboundedSender<WsMsg>,
}

#[derive(Clone, Default)]
pub struct Registry {
    /// Outbound channel to each connected po-k.
    pub conns: Arc<DashMap<PokId, PokConn>>,
    /// project name → owning po-k.
    pub project_to_pok: Arc<DashMap<String, PokId>>,
    /// session id → owning po-k.
    pub session_to_pok: Arc<DashMap<String, PokId>>,
    /// Pending unary requests awaiting a `ws_response`.
    pub pending: Arc<DashMap<Uuid, oneshot::Sender<WsResult>>>,
    /// Active streaming requests receiving `ws_stream_chunk`s.
    pub streams: Arc<DashMap<Uuid, mpsc::UnboundedSender<StreamFrame>>>,
    /// Pending `push_profile` requests awaiting a `profile_ack`.
    pub profile_acks: Arc<DashMap<Uuid, oneshot::Sender<String>>>,
}

impl Registry {
    /// Register (or re-register) a po-k connection, replacing its project rows.
    pub fn register(
        &self,
        conn: PokConn,
        projects: &[ProjectDecl],
        sessions: &[(String, String)],
    ) {
        let pok_id = conn.pok_id.clone();
        // Drop any stale project/session rows owned by this pok_id first.
        self.project_to_pok.retain(|_, v| v != &pok_id);
        self.session_to_pok.retain(|_, v| v != &pok_id);
        for p in projects {
            self.project_to_pok.insert(p.name.clone(), pok_id.clone());
        }
        for (sid, _project) in sessions {
            self.session_to_pok.insert(sid.clone(), pok_id.clone());
        }
        self.conns.insert(pok_id, conn);
    }

    /// Replace the project rows owned by `pok_id` (on `config_update`).
    pub fn update_projects(&self, pok_id: &str, projects: &[ProjectDecl]) {
        self.project_to_pok.retain(|_, v| v != pok_id);
        for p in projects {
            self.project_to_pok.insert(p.name.clone(), pok_id.to_string());
        }
    }

    pub fn disconnect(&self, pok_id: &str) {
        self.conns.remove(pok_id);
        self.project_to_pok.retain(|_, v| v != pok_id);
        self.session_to_pok.retain(|_, v| v != pok_id);
    }

    pub fn pok_for_session(&self, sid: &str) -> Option<PokId> {
        self.session_to_pok.get(sid).map(|r| r.clone())
    }

    pub fn pok_for_project(&self, project: &str) -> Option<PokId> {
        self.project_to_pok.get(project).map(|r| r.clone())
    }

    pub fn send(&self, pok_id: &str, msg: WsMsg) -> bool {
        if let Some(conn) = self.conns.get(pok_id) {
            conn.tx.send(msg).is_ok()
        } else {
            false
        }
    }

    /// `GET /registry` view.
    pub fn list(&self) -> Value {
        let instances: Vec<Value> = self
            .conns
            .iter()
            .map(|e| {
                let projects: Vec<String> = self
                    .project_to_pok
                    .iter()
                    .filter(|p| p.value() == e.key())
                    .map(|p| p.key().clone())
                    .collect();
                let sessions: Vec<String> = self
                    .session_to_pok
                    .iter()
                    .filter(|s| s.value() == e.key())
                    .map(|s| s.key().clone())
                    .collect();
                json!({
                    "pok_id": e.pok_id,
                    "hostname": e.hostname,
                    "version": e.version,
                    "projects": projects,
                    "sessions": sessions,
                })
            })
            .collect();
        json!(instances)
    }

    pub fn connected_count(&self) -> usize {
        self.conns.len()
    }
}
