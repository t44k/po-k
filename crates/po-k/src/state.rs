//! Shared HTTP service state. Wrapped in `Arc` and cloned into every handler.

use dashmap::DashMap;
use pok_proto::WsMsg;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, RwLock};

use crate::auth::Token;
use crate::config::Config;
use crate::event_bus::EventBus;
use crate::events_store::Db;
use crate::permissions::PermissionTracker;
use crate::session::Registry;

/// Sink for proactively forwarding events/status to Xpo-k over the WebSocket.
/// `None` until the WS client connects; swapped on reconnect.
pub type Uplink = Arc<Mutex<Option<UnboundedSender<WsMsg>>>>;

#[derive(Clone)]
pub struct AppState {
    pub token: Token,
    pub config: Arc<RwLock<Config>>,
    pub config_path: PathBuf,
    pub db: Db,
    pub sessions: Registry,
    pub bus: EventBus,
    pub perms: PermissionTracker,
    /// WebSocket uplink to Xpo-k (M14 Phase 2).
    pub uplink: Uplink,
    /// Last status pushed per session, so we only emit `status_update` on change.
    pub last_status: Arc<DashMap<String, String>>,
}

impl AppState {
    pub fn new(token: Token, config: Config, config_path: PathBuf, db: Db) -> Self {
        Self {
            token,
            config: Arc::new(RwLock::new(config)),
            config_path,
            db,
            sessions: Registry::default(),
            bus: EventBus::default(),
            perms: PermissionTracker::default(),
            uplink: Arc::new(Mutex::new(None)),
            last_status: Arc::new(DashMap::new()),
        }
    }

    pub async fn projects(&self) -> Vec<crate::config::Project> {
        self.config.read().await.projects.clone()
    }

    /// Send a message to Xpo-k if connected. Best-effort.
    pub async fn uplink_send(&self, msg: WsMsg) {
        if let Some(tx) = self.uplink.lock().await.as_ref() {
            let _ = tx.send(msg);
        }
    }
}
