//! Shared Xpo-k application state.

use std::sync::Arc;

use crate::auth::Token;
use crate::config::Config;
use crate::registry::Registry;
use crate::store::Db;

#[derive(Clone)]
pub struct XState {
    pub config: Arc<Config>,
    pub token: Token,
    pub db: Db,
    pub registry: Registry,
}

impl XState {
    pub fn new(config: Config, token: Token, db: Db) -> Self {
        Self {
            config: Arc::new(config),
            token,
            db,
            registry: Registry::default(),
        }
    }
}
