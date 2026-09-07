//! The hub: this po-k's view of *other* po-ks.
//!
//! An orchestrator (Hermes on ange) talks only to its local `po-k serve`. The
//! hub lets that local po-k (1) remember remote boxes and reach them with the
//! fleet bearer token, (2) proxy the whole session API to a remote box, and
//! (3) watch remote sessions and call a webhook on the local Hermes when a
//! turn finishes, needs input, ends, or the remote box stops answering.
//!
//! State lives in the local events.db (`hub_hosts`, `hub_watches`,
//! `hub_notifications`); watcher tasks are respawned from it at startup and
//! the deliverer drains whatever is pending or overdue.
//!
//! Notification loop: watcher persists a boundary (exactly once per watch and
//! boundary) → deliverer POSTs it to the webhook → the woken Hermes turn acks
//! it → until then the deliverer replays it after `ack_timeout`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::AbortHandle;

pub mod deliver;
pub mod hosts;
pub mod store;
pub mod watcher;
pub mod webhook;

#[derive(Clone)]
pub struct Hub {
    /// Shared client for remote po-k calls and webhook POSTs.
    pub client: reqwest::Client,
    /// Wakes the deliverer as soon as a notification is enqueued.
    pub delivery_wake: Arc<Notify>,
    tasks: Arc<Mutex<HashMap<String, AbortHandle>>>,
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

impl Hub {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent(concat!("po-k/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client");
        Self {
            client,
            delivery_wake: Arc::new(Notify::new()),
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Track a watcher task by watch id, aborting any previous task with the
    /// same id.
    pub async fn register_task(&self, id: &str, handle: AbortHandle) {
        if let Some(old) = self.tasks.lock().await.insert(id.to_string(), handle) {
            old.abort();
        }
    }

    pub async fn abort_task(&self, id: &str) -> bool {
        match self.tasks.lock().await.remove(id) {
            Some(h) => {
                h.abort();
                true
            }
            None => false,
        }
    }

    pub async fn forget_task(&self, id: &str) {
        self.tasks.lock().await.remove(id);
    }

    pub async fn running_task_ids(&self) -> Vec<String> {
        self.tasks.lock().await.keys().cloned().collect()
    }
}
