//! In-flight permission request tracker.
//!
//! When `po-k mcp` receives an `approve` tool call from CC, it HTTP-POSTs to
//! `POST /sessions/:id/mcp/approve`. The handler:
//!   1. inserts a `oneshot::Sender<Decision>` into this map keyed by a fresh
//!      `request_id`,
//!   2. emits a `permission_request` event on the events stream so the
//!      orchestrator sees it,
//!   3. awaits the oneshot up to `cc.permission_timeout` (default 60s).
//!
//! The orchestrator resolves by hitting
//! `POST /sessions/:id/permission_requests/:req_id` with `{behavior, message?}`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub behavior: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Decision {
    pub fn deny(message: impl Into<String>) -> Self {
        Self {
            behavior: "deny".into(),
            message: Some(message.into()),
        }
    }
}

#[derive(Clone, Default)]
pub struct PermissionTracker {
    inner: Arc<Mutex<HashMap<String, oneshot::Sender<Decision>>>>,
}

impl PermissionTracker {
    /// Register a new pending request; returns the receiver the handler waits on.
    pub async fn register(&self, request_id: String) -> oneshot::Receiver<Decision> {
        let (tx, rx) = oneshot::channel();
        self.inner.lock().await.insert(request_id, tx);
        rx
    }

    /// Resolve a pending request. Returns Ok(()) if it was found + signaled.
    pub async fn resolve(&self, request_id: &str, decision: Decision) -> Result<(), &'static str> {
        let tx = self
            .inner
            .lock()
            .await
            .remove(request_id)
            .ok_or("unknown or already-resolved request_id")?;
        tx.send(decision)
            .map_err(|_| "receiver dropped (handler likely timed out)")?;
        Ok(())
    }

    /// Drop a request (e.g. on timeout) so a late orchestrator response is
    /// quietly ignored.
    pub async fn forget(&self, request_id: &str) {
        self.inner.lock().await.remove(request_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_resolve_roundtrip() {
        let t = PermissionTracker::default();
        let rx = t.register("r1".into()).await;
        t.resolve("r1", Decision { behavior: "allow".into(), message: None })
            .await
            .unwrap();
        let d = rx.await.unwrap();
        assert_eq!(d.behavior, "allow");
    }

    #[tokio::test]
    async fn resolving_unknown_id_errors() {
        let t = PermissionTracker::default();
        let err = t
            .resolve("nope", Decision { behavior: "deny".into(), message: None })
            .await
            .unwrap_err();
        assert!(err.contains("unknown"));
    }

    #[tokio::test]
    async fn forget_drops_silently() {
        let t = PermissionTracker::default();
        let rx = t.register("r2".into()).await;
        t.forget("r2").await;
        drop(rx);
    }
}
