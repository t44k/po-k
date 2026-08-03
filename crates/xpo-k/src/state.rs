//! Shared Xpo-k application state.

use std::sync::Arc;

use crate::auth::Token;
use crate::config::Config;
use crate::registry::Registry;
use crate::store::Db;
use crate::subs::NotifyHub;

#[derive(Clone)]
pub struct XState {
    pub config: Arc<Config>,
    pub token: Token,
    pub db: Db,
    pub registry: Registry,
    /// Per-subscriber wakeups for notification long-polls (M15).
    pub notify_hub: NotifyHub,
}

impl XState {
    pub fn new(config: Config, token: Token, db: Db) -> Self {
        Self {
            config: Arc::new(config),
            token,
            db,
            registry: Registry::default(),
            notify_hub: NotifyHub::default(),
        }
    }
}
