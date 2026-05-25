//! Per-session `Notify` so long-poll + SSE handlers wake on new events.
//!
//! Writers (hook ingest, jsonl tailer, spawn/kill, permission tracker) call
//! `bus.notify(sid)` after they commit a row. Readers call `bus.subscribe(sid)`
//! once to get an `Arc<Notify>` and then `notified().await` between polls.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Default)]
pub struct EventBus {
    inner: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl EventBus {
    pub async fn notify(&self, sid: &str) {
        if let Some(n) = self.inner.lock().await.get(sid) {
            n.notify_waiters();
        }
    }

    pub async fn subscribe(&self, sid: &str) -> Arc<Notify> {
        let mut guard = self.inner.lock().await;
        guard
            .entry(sid.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub async fn drop_session(&self, sid: &str) {
        self.inner.lock().await.remove(sid);
    }
}
