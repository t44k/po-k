//! The deliverer: one task whose only job is to get notification rows to the
//! webhook and keep re-sending them until the orchestrator acknowledges them.
//!
//! Per row: `pending` → POST → `delivered` (boundary events) or `acked`
//! (informational events). A `delivered` row whose `ack_timeout` elapsed is
//! posted again with a fresh `x-request-id` (`<id>:<attempt>`), the interval
//! doubling up to `REPLAY_MAX_INTERVAL`; after `REPLAY_MAX_ATTEMPTS` it is
//! parked as `failed` (still visible through `GET /notifications`). Transport
//! errors and 5xx back off 30 s → 15 m; a 4xx is a configuration problem and
//! parks the row immediately.
//!
//! Runs from startup, so everything pending or overdue when `po-k serve`
//! restarts is delivered without anyone asking.

use serde_json::{json, Value};

use super::store::{self, NotificationRow};
use super::webhook::{self, Outcome};
use crate::defaults;
use crate::events_store::{iso_in, now_iso};
use crate::state::AppState;

/// Rows handled per pass.
pub const BATCH: i64 = 20;

/// Seconds until the next replay of a delivered-but-unacked row, given how
/// many deliveries happened so far (>= 1).
pub fn replay_secs(ack_timeout_secs: i64, deliveries: i64) -> i64 {
    let base = ack_timeout_secs.max(defaults::ACK_TIMEOUT_MIN_SECS);
    let doubled = base.saturating_mul(1i64 << (deliveries.max(1) - 1).min(20));
    doubled.min(defaults::REPLAY_MAX_INTERVAL.as_secs() as i64)
}

/// The webhook body for one delivery attempt. Metadata only — never CC prose.
pub fn envelope(n: &NotificationRow, attempt: i64, unacked_previous: &[String]) -> Value {
    json!({
        "event_type": webhook::EVENT_TYPE,
        "notification_id": n.id,
        "attempt": attempt,
        "first_delivered_at": n.first_delivered_at,
        "event": n.event,
        "host": n.host,
        "session_id": n.session_id,
        "watch_id": n.watch_id,
        "status": n.status,
        "boundary_cursor": n.boundary_cursor,
        "deciding_event": n.deciding_event,
        "message": n.message,
        "origin": n.origin,
        "requires_ack": n.requires_ack,
        "unacked_previous": unacked_previous,
        "ts": now_iso(),
    })
}

pub fn request_id(notification_id: &str, attempt: i64) -> String {
    format!("{notification_id}:{attempt}")
}

/// One pass over the due rows. Returns `(attempted, delivered)`.
pub async fn pass(state: &AppState) -> (usize, usize) {
    let due = match store::due_notifications(&state.db, &now_iso(), BATCH).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "deliverer: cannot list due notifications");
            return (0, 0);
        }
    };
    let attempted = due.len();
    let mut delivered = 0;
    for n in due {
        if deliver_one(state, &n).await {
            delivered += 1;
        }
    }
    (attempted, delivered)
}

async fn deliver_one(state: &AppState, n: &NotificationRow) -> bool {
    let watch = match store::get_watch(&state.db, &n.watch_id).await {
        Ok(Some(w)) => w,
        _ => {
            let _ = store::mark_failed(&state.db, &n.id, "watch row is gone").await;
            return false;
        }
    };
    if watch.state == "stopped" {
        let _ = store::cancel_for_watch(&state.db, &watch.id).await;
        return false;
    }
    let attempt = n.attempts + 1;
    if attempt > defaults::REPLAY_MAX_ATTEMPTS {
        let msg = format!("not acknowledged after {} deliveries", n.attempts);
        tracing::error!(notification = %n.id, watch = %n.watch_id, sid = %n.session_id, event = %n.event, "{msg}");
        let _ = store::mark_failed(&state.db, &n.id, &msg).await;
        return false;
    }
    let secret = match webhook::resolve_secret(&watch.webhook) {
        Ok(s) => s,
        Err(e) => {
            // A missing secret is a config error, not a transient fault.
            tracing::error!(notification = %n.id, error = %e, "deliverer: webhook secret unavailable");
            let _ = store::mark_failed(&state.db, &n.id, &e).await;
            return false;
        }
    };
    let previous = store::unacked_ids_for_watch(&state.db, &n.watch_id, &n.id).await.unwrap_or_default();
    let body = match serde_json::to_vec(&envelope(n, attempt, &previous)) {
        Ok(b) => b,
        Err(e) => {
            let _ = store::mark_failed(&state.db, &n.id, &format!("cannot serialise envelope: {e}")).await;
            return false;
        }
    };
    let rid = request_id(&n.id, attempt);
    match webhook::post_once(&state.hub.client, &watch.webhook, &rid, &body, &secret).await {
        o @ (Outcome::Delivered | Outcome::Duplicate) => {
            // `attempt` counts deliveries so far including this one.
            let next = iso_in(replay_secs(watch.ack_timeout_secs, attempt));
            let _ = store::mark_delivered(&state.db, &n.id, n.requires_ack, &next).await;
            tracing::info!(notification = %n.id, watch = %n.watch_id, sid = %n.session_id, event = %n.event, attempt, ?o,
                replay_at = %if n.requires_ack { next.as_str() } else { "-" }, "webhook delivered");
            true
        }
        Outcome::Retry(e) => {
            let next = iso_in(webhook::backoff_secs(attempt));
            tracing::warn!(notification = %n.id, event = %n.event, attempt, error = %e, retry_at = %next, "webhook delivery failed; will retry");
            let _ = store::mark_retry(&state.db, &n.id, &e, &next).await;
            let _ = store::record_error(&state.db, &n.watch_id, &format!("webhook {}: {e}", n.event)).await;
            false
        }
        Outcome::Fatal(e) => {
            tracing::error!(notification = %n.id, event = %n.event, error = %e, "webhook rejected permanently; notification parked as failed");
            let _ = store::mark_failed(&state.db, &n.id, &e).await;
            let _ = store::record_error(&state.db, &n.watch_id, &format!("webhook {}: {e}", n.event)).await;
            false
        }
    }
}

/// Spawn the delivery loop. Wakes on `hub.delivery_wake` (instant push) and
/// otherwise re-checks every `DELIVERY_TICK` so backoff expiries, replays and
/// restarts still drain.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        loop {
            // Arm the waiter BEFORE the pass so a row enqueued mid-pass still
            // wakes the next iteration (`notify_waiters` leaves no permit).
            let wake = state.hub.delivery_wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let (attempted, delivered) = pass(&state).await;
            if attempted > 0 {
                tracing::debug!(attempted, delivered, "deliverer pass");
            }
            let _ = tokio::time::timeout(defaults::DELIVERY_TICK, wake).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::store::{WatchRow, WebhookTarget};
    use axum::extract::State as AxState;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex};

    #[test]
    fn replay_interval_doubles_from_ack_timeout_and_caps() {
        assert_eq!(replay_secs(900, 1), 900);
        assert_eq!(replay_secs(900, 2), 1800);
        assert_eq!(replay_secs(900, 3), 3600);
        assert_eq!(replay_secs(900, 10), 3600);
        assert_eq!(replay_secs(5, 1), defaults::ACK_TIMEOUT_MIN_SECS);
        assert_eq!(request_id("n-1", 3), "n-1:3");
    }

    #[derive(Clone, Default)]
    struct Receiver {
        posts: Arc<Mutex<Vec<(HeaderMap, Value)>>>,
        status: Arc<Mutex<u16>>,
    }

    async fn stub_receiver(status: u16) -> (String, Receiver) {
        let recv = Receiver { posts: Arc::new(Mutex::new(vec![])), status: Arc::new(Mutex::new(status)) };
        let app = Router::new()
            .route(
                "/webhooks/pok",
                post(|AxState(r): AxState<Receiver>, headers: HeaderMap, Json(body): Json<Value>| async move {
                    r.posts.lock().unwrap().push((headers, body));
                    let code = *r.status.lock().unwrap();
                    (axum::http::StatusCode::from_u16(code).unwrap(), Json(json!({ "status": "accepted" })))
                }),
            )
            .with_state(recv.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/webhooks/pok"), recv)
    }

    async fn watch_for(state: &AppState, url: &str) -> WatchRow {
        std::env::set_var("POK_TEST_DELIVER_SECRET", "s3cret");
        let wh = WebhookTarget { url: url.into(), secret_env: Some("POK_TEST_DELIVER_SECRET".into()), secret_file: None };
        store::insert_watch(&state.db, "box", "sid-1", &wh, &json!({ "platform": "zulip", "chat_id": "stream:eng", "thread_id": "t" }), 0, 60)
            .await
            .unwrap()
    }

    fn body_and_rid(r: &Receiver, i: usize) -> (Value, String) {
        let posts = r.posts.lock().unwrap();
        let (h, b) = &posts[i];
        (b.clone(), h.get("x-request-id").unwrap().to_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn boundary_notification_is_delivered_replayed_until_acked_and_never_twice_per_boundary() {
        let state = crate::http::test_support::test_state().await;
        let (url, recv) = stub_receiver(202).await;
        let w = watch_for(&state, &url).await;
        let origin = json!({ "chat_id": "stream:eng" });
        let n = store::enqueue(&state.db, &w, "finished", Some("idle"), 7, &json!({ "kind": "stop", "seq": 7 }), None, &origin).await.unwrap().unwrap();
        // The same boundary observed again produces nothing new.
        assert!(store::enqueue(&state.db, &w, "finished", Some("idle"), 7, &Value::Null, None, &origin).await.unwrap().is_none());

        assert_eq!(pass(&state).await, (1, 1));
        let (body, rid) = body_and_rid(&recv, 0);
        assert_eq!(rid, format!("{}:1", n.id));
        assert_eq!(body["notification_id"], n.id);
        assert_eq!(body["attempt"], 1);
        assert_eq!(body["event"], "finished");
        assert_eq!(body["boundary_cursor"], 7);
        assert_eq!(body["origin"]["chat_id"], "stream:eng");
        assert_eq!(body["requires_ack"], true);
        let row = store::get_notification(&state.db, &n.id).await.unwrap().unwrap();
        assert_eq!(row.state, "delivered");
        // Not due again until the ack timeout passes.
        assert_eq!(pass(&state).await, (0, 0));

        // Unacknowledged past the timeout → replayed with a fresh request id.
        store::force_due(&state.db, &n.id).await.unwrap();
        assert_eq!(pass(&state).await, (1, 1));
        let (body2, rid2) = body_and_rid(&recv, 1);
        assert_eq!(rid2, format!("{}:2", n.id));
        assert_eq!(body2["attempt"], 2);
        assert!(body2["first_delivered_at"].is_string());

        // Ack stops the replays for good.
        assert_eq!(store::ack(&state.db, &n.id).await.unwrap(), Some(true));
        store::force_due(&state.db, &n.id).await.unwrap();
        assert_eq!(pass(&state).await, (0, 0));
        assert_eq!(recv.posts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn transient_failure_backs_off_and_fatal_parks() {
        let state = crate::http::test_support::test_state().await;
        let (url, recv) = stub_receiver(503).await;
        let w = watch_for(&state, &url).await;
        let origin = json!({});
        let n = store::enqueue(&state.db, &w, "finished", Some("idle"), 1, &Value::Null, None, &origin).await.unwrap().unwrap();
        assert_eq!(pass(&state).await, (1, 0));
        let row = store::get_notification(&state.db, &n.id).await.unwrap().unwrap();
        assert_eq!(row.state, "pending");
        assert_eq!(row.attempts, 1);
        assert!(row.next_attempt_at.as_deref().unwrap() > now_iso().as_str());
        assert!(row.last_error.as_deref().unwrap().contains("503"));
        // Backoff not elapsed → not retried this pass.
        assert_eq!(pass(&state).await, (0, 0));

        // Receiver comes back → delivered on the next due pass.
        *recv.status.lock().unwrap() = 202;
        store::force_due(&state.db, &n.id).await.unwrap();
        assert_eq!(pass(&state).await, (1, 1));
        let (_, rid) = body_and_rid(&recv, 1);
        assert_eq!(rid, format!("{}:2", n.id));

        // A 4xx is a config error: parked, no retry.
        *recv.status.lock().unwrap() = 401;
        let n2 = store::enqueue(&state.db, &w, "needs_input", Some("awaiting_input"), 3, &Value::Null, None, &origin).await.unwrap().unwrap();
        assert_eq!(pass(&state).await, (1, 0));
        let row = store::get_notification(&state.db, &n2.id).await.unwrap().unwrap();
        assert_eq!(row.state, "failed");
        assert!(row.last_error.as_deref().unwrap().contains("401"));
    }

    #[tokio::test]
    async fn informational_events_auto_ack_and_unacked_previous_is_reported() {
        let state = crate::http::test_support::test_state().await;
        let (url, recv) = stub_receiver(202).await;
        let w = watch_for(&state, &url).await;
        let origin = json!({});
        let first = store::enqueue(&state.db, &w, "finished", Some("idle"), 2, &Value::Null, None, &origin).await.unwrap().unwrap();
        let lost = store::enqueue(&state.db, &w, "connection_lost", None, 2, &Value::Null, Some("down"), &origin).await.unwrap().unwrap();
        let second = store::enqueue(&state.db, &w, "finished", Some("idle"), 5, &Value::Null, None, &origin).await.unwrap().unwrap();
        assert_eq!(pass(&state).await, (3, 3));
        assert_eq!(store::get_notification(&state.db, &lost.id).await.unwrap().unwrap().state, "acked");
        assert_eq!(store::get_notification(&state.db, &first.id).await.unwrap().unwrap().state, "delivered");
        let (body_second, _) = body_and_rid(&recv, 2);
        assert_eq!(body_second["notification_id"], second.id);
        assert_eq!(body_second["unacked_previous"], json!([first.id]));
        // A stopped watch cancels everything still outstanding.
        store::set_state(&state.db, &w.id, "stopped", None, None).await.unwrap();
        store::force_due(&state.db, &first.id).await.unwrap();
        assert_eq!(pass(&state).await, (1, 0));
        assert_eq!(store::get_notification(&state.db, &first.id).await.unwrap().unwrap().state, "cancelled");
    }
}
