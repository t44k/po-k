//! Shared service state, cloned into every handler and background task.

use std::sync::Arc;

use crate::auth::Token;
use crate::config::Config;
use crate::event_bus::EventBus;
use crate::events_store::Db;
use crate::hub::Hub;
use crate::permissions::PermissionTracker;
use crate::session::Registry;

#[derive(Clone)]
pub struct AppState {
    pub token: Token,
    pub config: Arc<Config>,
    pub db: Db,
    pub sessions: Registry,
    pub bus: EventBus,
    pub perms: PermissionTracker,
    /// Remote-box hub: HTTP client + running watcher tasks.
    pub hub: Hub,
}

impl AppState {
    pub fn new(token: Token, config: Config, db: Db) -> Self {
        Self {
            token,
            config: Arc::new(config),
            db,
            sessions: Registry::default(),
            bus: EventBus::default(),
            perms: PermissionTracker::default(),
            hub: Hub::new(),
        }
    }
}
