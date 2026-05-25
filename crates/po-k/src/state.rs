//! Shared HTTP service state. Wrapped in `Arc` and cloned into every handler.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::auth::Token;
use crate::config::Config;
use crate::event_bus::EventBus;
use crate::events_store::Db;
use crate::permissions::PermissionTracker;
use crate::session::Registry;

#[derive(Clone)]
pub struct AppState {
    pub token: Token,
    pub config: Arc<RwLock<Config>>,
    pub config_path: PathBuf,
    pub db: Db,
    pub sessions: Registry,
    pub bus: EventBus,
    pub perms: PermissionTracker,
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
        }
    }

    pub async fn projects(&self) -> Vec<crate::config::Project> {
        self.config.read().await.projects.clone()
    }
}
