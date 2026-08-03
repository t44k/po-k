//! Outbound webhook delivery for notification subscriptions (M16).
//!
//! Push is the primary path: the moment po-k reports a completion (or another
//! actionable event) for a subscribed session, Xpo-k signs a small metadata
//! envelope and POSTs it to Hermes' webhook adapter, which starts a fresh,
//! isolated agent turn. The durable queue built in M15 stays the source of
//! truth, so the roughly-hourly cron fallback can recover anything the push
//! never delivered.
//!
//! Invariants this module holds:
//!
//! * **Metadata only.** The body carries ids, seq, kind/status — never CC
//!   prose. The woken turn fetches session content itself via `pok_events`,
//!   which keeps untrusted model output out of the trigger path.
//! * **Delivery never acks.** `delivered` means "Hermes was told"; acking means
//!   "Hermes handled it" and stays the agent's explicit act. Every failure
//!   leaves the notification unacked and pollable.
//! * **The signature covers the exact bytes sent.** The body is serialised
//!   once, HMAC-SHA256'd, and that same buffer is written to the socket.
//! * **Idempotency is the receiver's job, keyed on ours.** `X-Request-ID` is
//!   the notification id, which Hermes' webhook adapter uses to collapse
//!   duplicate deliveries into a single turn.
//! * **Secrets are referenced, never stored.** Only the env-var name or file
//!   path lives in the database; the value is read at send time and never
//!   logged or returned by the API.

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::time::Duration;

use crate::state::XState;
use crate::store::now_epoch;
use crate::subs::{self, DueDelivery, NotificationRow};

/// Per-request timeout. Deliberately short: a wedged receiver must not pin the
/// delivery loop, and the retry schedule covers a slow restart.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the loop looks for due work when nothing wakes it.
pub const IDLE_TICK: Duration = Duration::from_secs(15);
/// Attempts per notification before it is parked as `failed` (still pollable).
pub const MAX_ATTEMPTS: i64 = 8;
/// Notifications pushed per pass.
pub const BATCH: i64 = 20;
/// The `event_type` the envelope declares, so a Hermes route can filter on it.
pub const EVENT_TYPE: &str = "pok_notification";

/// Retry delay after `attempts` failed attempts: 30s, 60s, 2m, 4m, 8m, then a
/// 15m ceiling. Bounded so a long receiver outage is cheap, and the cron
/// fallback is what actually guarantees eventual handling.
pub fn backoff_secs(attempts: i64) -> i64 {
    match attempts {
        a if a <= 1 => 30,
        2 => 60,
        3 => 120,
        4 => 240,
        5 => 480,
        _ => 900,
    }
}

/// Hex HMAC-SHA256 of `body` under `secret` — the `X-Webhook-Signature` value
/// Hermes' generic webhook validator expects.
pub fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .fold(String::with_capacity(64), |mut acc, b| {
            use std::fmt::Write;
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

/// The push envelope: metadata only, and enough of it that the woken turn knows
/// exactly what to poll, inspect, and report back to.
///
/// `origin.*` keys are ALWAYS present (empty string when unknown) — a Hermes
/// route templates `deliver_extra.chat_id: "{origin.chat_id}"` from them, and an
/// absent key would render as the literal `{origin.chat_id}` instead of falling
/// back to the platform's home channel.
pub fn build_body(n: &NotificationRow, workflow_id: Option<&str>, origin: &Value) -> Value {
    let field = |key: &str| -> String {
        origin
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    json!({
        "event_type": EVENT_TYPE,
        "notification_id": n.id,
        "subscription_id": n.subscription_id,
        "workflow_id": workflow_id.unwrap_or_default(),
        "subscriber": n.subscriber,
        "session_id": n.session_id,
        "seq": n.seq,
        "kind": n.kind,
        "status": n.status,
        "created_at": n.created_at,
        "origin": {
            "platform": field("platform"),
            "chat_id": field("chat_id"),
            "chat_name": field("chat_name"),
            "thread_id": field("thread_id"),
            "user_id": field("user_id"),
            "user_name": field("user_name"),
            "session_key": field("session_key"),
            "hint": field("hint"),
        },
    })
}

/// What to do with a receiver's response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Accepted — a turn was started (or the receiver already had it).
    Delivered,
    /// The receiver recognised this delivery id and did not start a turn.
    /// Success from our side: exactly one turn exists for the notification.
    Duplicate,
    /// Transient; try again later.
    Retry(String),
    /// Permanent (bad route, bad signature, bad request). Retrying cannot help,
    /// so park it and let the operator + cron fallback take over.
    Fatal(String),
}

/// Classify an HTTP response. 2xx is success; the adapter's
/// `{"status":"duplicate"}` (also 2xx) is reported separately so operators can
/// see push/duplicate ratios.
pub fn classify(status: u16, body: &str) -> Outcome {
    if (200..300).contains(&status) {
        let duplicate = serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|v| {
                v.get("status")
                    .and_then(|s| s.as_str())
                    .map(|s| s == "duplicate")
            })
            .unwrap_or(false);
        return if duplicate {
            Outcome::Duplicate
        } else {
            Outcome::Delivered
        };
    }
    match status {
        // Auth/route/shape problems: the operator has to fix config.
        400 | 401 | 403 | 404 | 405 | 410 | 422 => {
            Outcome::Fatal(format!("HTTP {status}: {}", snippet(body)))
        }
        // 408/429/5xx and anything else: transient.
        _ => Outcome::Retry(format!("HTTP {status}: {}", snippet(body))),
    }
}

fn snippet(body: &str) -> String {
    body.chars().take(160).collect()
}

/// Read the HMAC secret for a due delivery. Env var wins over file. The value
/// is never logged; only its absence is reported.
pub fn resolve_secret(due: &DueDelivery) -> Result<String, String> {
    if let Some(name) = due.secret_env.as_deref().filter(|n| !n.is_empty()) {
        return match std::env::var(name) {
            Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
            _ => Err(format!("secret env var {name:?} is unset or empty")),
        };
    }
    if let Some(path) = due.secret_file.as_deref().filter(|p| !p.is_empty()) {
        return match std::fs::read_to_string(path) {
            Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
            Ok(_) => Err(format!("secret file {path:?} is empty")),
            Err(e) => Err(format!("secret file {path:?} unreadable: {e}")),
        };
    }
    Err("no secret_env or secret_file configured".into())
}

/// POST one notification. Returns the classified outcome; never panics and
/// never touches ack state.
pub async fn deliver_once(client: &reqwest::Client, due: &DueDelivery, secret: &str) -> Outcome {
    // Serialise ONCE — the signature must cover the exact bytes sent.
    let body = match serde_json::to_vec(&build_body(
        &due.notification,
        due.workflow_id.as_deref(),
        &due.origin,
    )) {
        Ok(b) => b,
        Err(e) => return Outcome::Fatal(format!("cannot serialise envelope: {e}")),
    };
    let signature = sign_body(secret, &body);
    let res = client
        .post(&due.url)
        .header("content-type", "application/json")
        .header("x-webhook-signature", signature)
        .header("x-request-id", &due.notification.id)
        .header("x-pok-event", EVENT_TYPE)
        .timeout(REQUEST_TIMEOUT)
        .body(body)
        .send()
        .await;
    match res {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            classify(status, &text)
        }
        Err(e) if e.is_timeout() => Outcome::Retry(format!("timeout after {REQUEST_TIMEOUT:?}")),
        Err(e) => Outcome::Retry(format!("request failed: {e}")),
    }
}

/// Apply an outcome to the durable delivery state. Returns the state written,
/// for logging/tests.
pub async fn apply_outcome(
    state: &XState,
    due: &DueDelivery,
    outcome: &Outcome,
) -> anyhow::Result<&'static str> {
    let id = &due.notification.id;
    match outcome {
        Outcome::Delivered | Outcome::Duplicate => {
            subs::mark_delivered(&state.db, id).await?;
            Ok("delivered")
        }
        Outcome::Retry(err) => {
            let attempts = due.notification.delivery_attempts + 1;
            if attempts >= MAX_ATTEMPTS {
                subs::record_failure(
                    &state.db,
                    id,
                    &format!("giving up after {attempts} attempts: {err}"),
                    None,
                )
                .await?;
                Ok("failed")
            } else {
                let retry_at = now_epoch() + backoff_secs(attempts);
                subs::record_failure(&state.db, id, err, Some(retry_at)).await?;
                Ok("pending")
            }
        }
        Outcome::Fatal(err) => {
            subs::record_failure(&state.db, id, err, None).await?;
            Ok("failed")
        }
    }
}

/// Run one delivery pass. Returns `(attempted, delivered)`.
pub async fn run_pass(state: &XState, client: &reqwest::Client) -> (usize, usize) {
    let due = match subs::due_deliveries(&state.db, now_epoch(), BATCH).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "webhook delivery: cannot list due notifications");
            return (0, 0);
        }
    };
    let mut delivered = 0;
    let attempted = due.len();
    for item in due {
        let outcome = match resolve_secret(&item) {
            Ok(secret) => deliver_once(client, &item, &secret).await,
            // A missing secret is a config error, not a transient fault.
            Err(e) => Outcome::Fatal(e),
        };
        match &outcome {
            Outcome::Delivered | Outcome::Duplicate => delivered += 1,
            Outcome::Retry(e) => tracing::warn!(
                notification = %item.notification.id, sid = %item.notification.session_id,
                attempts = item.notification.delivery_attempts + 1, error = %e,
                "webhook delivery failed; will retry (notification stays pollable)"
            ),
            Outcome::Fatal(e) => tracing::error!(
                notification = %item.notification.id, sid = %item.notification.session_id,
                error = %e,
                "webhook delivery rejected permanently; notification stays unacked \
                 for the cron fallback"
            ),
        }
        if let Err(e) = apply_outcome(state, &item, &outcome).await {
            tracing::warn!(error = %e, "webhook delivery: cannot persist outcome");
        }
    }
    (attempted, delivered)
}

/// Spawn the delivery loop. It wakes immediately when `state.delivery_wake` is
/// notified (that is what makes push feel instant) and otherwise re-checks on
/// `IDLE_TICK` so a restart, a backoff expiry, or a missed wake still drains.
pub fn spawn(state: XState) {
    let client = match reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(concat!("xpo-k/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "webhook delivery disabled: cannot build HTTP client");
            return;
        }
    };
    tokio::spawn(async move {
        loop {
            // Arm the waiter BEFORE the pass so a notification enqueued while
            // we are working still wakes the next iteration (`notify_waiters`
            // leaves no permit for an unarmed waiter).
            let wake = state.delivery_wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();

            let (attempted, delivered) = run_pass(&state, &client).await;
            if attempted > 0 {
                tracing::debug!(attempted, delivered, "webhook delivery pass");
            }
            let _ = tokio::time::timeout(IDLE_TICK, wake).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subs::NotificationRow;

    fn notif(attempts: i64) -> NotificationRow {
        NotificationRow {
            id: "ntf-abc".into(),
            subscription_id: "sub-1".into(),
            subscriber: "hermes-1".into(),
            session_id: "sess-9".into(),
            seq: 214,
            kind: "stop".into(),
            status: None,
            // Deliberately carries CC prose: `build_body` must not forward it.
            payload: json!({"event": {"payload": {"last_assistant_message": "SECRET PROSE"}}}),
            created_at: "2026-08-03T10:00:00Z".into(),
            delivery_state: "pending".into(),
            delivery_attempts: attempts,
        }
    }

    fn test_origin() -> Value {
        json!({
            "platform": "zulip",
            "chat_id": "stream:eng",
            "thread_id": "deploy-bug",
            "user_name": "Tamas",
        })
    }

    fn due(attempts: i64) -> DueDelivery {
        DueDelivery {
            notification: notif(attempts),
            url: "http://127.0.0.1:1/webhooks/pok".into(),
            secret_env: Some("POK_TEST_SECRET".into()),
            secret_file: None,
            workflow_id: Some("wf-1".into()),
            origin: test_origin(),
        }
    }

    /// Known-answer test: HMAC-SHA256("key", "The quick brown fox jumps over the
    /// lazy dog") from RFC-style test vectors.
    #[test]
    fn sign_body_matches_known_hmac_vector() {
        assert_eq!(
            sign_body("key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        // Empty key and empty body are still valid HMAC inputs.
        assert_eq!(
            sign_body("", b""),
            "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
        );
    }

    #[test]
    fn signature_covers_the_exact_body_bytes() {
        let body =
            serde_json::to_vec(&build_body(&notif(0), Some("wf-1"), &test_origin())).unwrap();
        let sig = sign_body("s3cret", &body);
        // Same bytes → same signature; one flipped byte → different signature.
        assert_eq!(sig, sign_body("s3cret", &body));
        let mut tampered = body.clone();
        tampered.push(b' ');
        assert_ne!(sig, sign_body("s3cret", &tampered));
        // …and a different secret does not validate.
        assert_ne!(sig, sign_body("other", &body));
    }

    #[test]
    fn body_is_metadata_only_and_carries_no_cc_prose() {
        let body = build_body(&notif(0), Some("wf-1"), &test_origin());
        let raw = serde_json::to_string(&body).unwrap();
        assert!(!raw.contains("SECRET PROSE"), "CC prose leaked: {raw}");
        assert!(!raw.contains("payload"), "payload forwarded: {raw}");
        assert!(!raw.contains("last_assistant_message"));
        for key in [
            "event_type",
            "notification_id",
            "subscription_id",
            "subscriber",
            "session_id",
            "seq",
            "kind",
            "status",
            "created_at",
        ] {
            assert!(body.get(key).is_some(), "missing {key} in {raw}");
        }
        assert_eq!(body["event_type"], EVENT_TYPE);
        assert_eq!(body["notification_id"], "ntf-abc");
        assert_eq!(body["seq"], 214);
        // Exactly the declared keys — nothing else rides along.
        assert_eq!(body.as_object().unwrap().len(), 11);
    }

    #[test]
    fn envelope_carries_workflow_and_origin_for_reply_routing() {
        let body = build_body(&notif(0), Some("wf-42"), &test_origin());
        assert_eq!(body["workflow_id"], "wf-42");
        assert_eq!(body["origin"]["platform"], "zulip");
        assert_eq!(body["origin"]["chat_id"], "stream:eng");
        assert_eq!(body["origin"]["thread_id"], "deploy-bug");
        assert_eq!(body["origin"]["user_name"], "Tamas");
    }

    /// Every templated origin key must exist even when unknown: Hermes' route
    /// renders `{origin.chat_id}` literally if the key is absent, which would
    /// send the report to a channel named "{origin.chat_id}" instead of falling
    /// back to the platform home channel.
    #[test]
    fn origin_keys_are_always_present_so_templates_never_render_literally() {
        let body = build_body(&notif(0), None, &json!({}));
        assert_eq!(body["workflow_id"], "", "empty, not missing");
        let origin = body["origin"].as_object().expect("origin object");
        for key in [
            "platform",
            "chat_id",
            "chat_name",
            "thread_id",
            "user_id",
            "user_name",
            "session_key",
            "hint",
        ] {
            assert_eq!(
                origin.get(key).and_then(|v| v.as_str()),
                Some(""),
                "origin.{key} must be present and empty"
            );
        }
        // A partially-filled origin keeps what it has and blanks the rest.
        let partial = build_body(&notif(0), None, &json!({"chat_id": "stream:ops"}));
        assert_eq!(partial["origin"]["chat_id"], "stream:ops");
        assert_eq!(partial["origin"]["thread_id"], "");
    }

    #[test]
    fn backoff_is_monotonic_and_capped() {
        let seq: Vec<i64> = (1..=9).map(backoff_secs).collect();
        assert_eq!(seq, vec![30, 60, 120, 240, 480, 900, 900, 900, 900]);
        assert!(seq.windows(2).all(|w| w[1] >= w[0]), "must not shrink");
        assert_eq!(backoff_secs(0), 30, "defensive: attempt 0 behaves like 1");
    }

    #[test]
    fn classify_maps_responses_to_outcomes() {
        assert_eq!(
            classify(200, r#"{"status":"accepted"}"#),
            Outcome::Delivered
        );
        assert_eq!(classify(202, ""), Outcome::Delivered);
        assert_eq!(
            classify(200, r#"{"status":"duplicate","delivery_id":"ntf-abc"}"#),
            Outcome::Duplicate
        );
        // Config errors are permanent…
        for s in [400u16, 401, 403, 404, 405, 410, 422] {
            assert!(
                matches!(classify(s, "nope"), Outcome::Fatal(_)),
                "HTTP {s} should be fatal"
            );
        }
        // …everything else is worth retrying.
        for s in [408u16, 429, 500, 502, 503, 504, 599] {
            assert!(
                matches!(classify(s, "boom"), Outcome::Retry(_)),
                "HTTP {s} should retry"
            );
        }
        // A huge error body is truncated into the outcome.
        let big = "x".repeat(5000);
        if let Outcome::Retry(msg) = classify(500, &big) {
            assert!(msg.len() < 300, "error text must be bounded: {}", msg.len());
        } else {
            panic!("expected retry");
        }
    }

    #[test]
    fn resolve_secret_prefers_env_then_file_and_never_returns_a_blank() {
        // SAFETY: single-threaded test, restored before returning.
        std::env::set_var("POK_TEST_SECRET", "  from-env  ");
        assert_eq!(resolve_secret(&due(0)).unwrap(), "from-env");
        std::env::set_var("POK_TEST_SECRET", "   ");
        assert!(
            resolve_secret(&due(0)).is_err(),
            "blank env is not a secret"
        );
        std::env::remove_var("POK_TEST_SECRET");
        let err = resolve_secret(&due(0)).unwrap_err();
        assert!(err.contains("POK_TEST_SECRET"), "{err}");
        assert!(!err.contains("from-env"), "must not echo the value");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, "from-file\n").unwrap();
        let mut d = due(0);
        d.secret_env = None;
        d.secret_file = Some(path.to_string_lossy().into());
        assert_eq!(resolve_secret(&d).unwrap(), "from-file");

        let mut none = due(0);
        none.secret_env = None;
        none.secret_file = None;
        assert!(resolve_secret(&none).unwrap_err().contains("no secret"));
    }
}
