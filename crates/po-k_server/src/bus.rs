//! Per-session event bus for live transcript updates.
//!
//! On `/ingest` commit, the server publishes each accepted event's rendered HTML
//! snippet to a `tokio::sync::broadcast` keyed by `session_key`. WebSocket
//! subscribers on `/ui/session/:key/ws` receive each snippet and append it to
//! the live transcript. Closed channels (no subscribers) are pruned lazily.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct EventBus {
    senders: Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe(&self, session_key: &str) -> broadcast::Receiver<String> {
        let mut map = self.senders.lock().expect("bus lock");
        let sender = map
            .entry(session_key.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }

    pub fn publish(&self, session_key: &str, html: String) {
        // Take the lock briefly to look up the sender; do the send unlocked.
        let sender = {
            let map = self.senders.lock().expect("bus lock");
            map.get(session_key).cloned()
        };
        if let Some(sender) = sender {
            // `send` only errors when there are no receivers; that's fine.
            let _ = sender.send(html);
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
