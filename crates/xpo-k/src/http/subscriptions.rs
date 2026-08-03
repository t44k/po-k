//! Subscription + notification endpoints (M15).
//!
//! These are the only orchestrator-facing endpoints that are served entirely
//! from Xpo-k's own state — no po-k round trip — so an idle orchestrator can
//! collect completions cheaply, and a po-k hiccup can't stall the poll.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

use crate::state::XState;
use crate::subs;

type Resp = (StatusCode, Json<Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> Resp {
    (code, Json(json!({ "error": msg.into() })))
}

fn internal<E: std::fmt::Display>(e: E) -> Resp {
    err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

/// Longest a `GET /notifications` long-poll may park, in seconds.
const MAX_POLL_WAIT: u64 = 60;
/// How long acked notifications are retained (idempotent re-ack window).
const ACK_RETENTION_SECS: i64 = 3600;

#[derive(Debug, Deserialize)]
pub struct CreateBody {
    pub session_id: String,
    #[serde(default)]
    pub subscriber: Option<String>,
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default)]
    pub ttl_secs: Option<i64>,
    /// Start from this event seq instead of the session's current cursor.
    /// `0` replays everything po-k still holds for the session.
    #[serde(default)]
    pub cursor: Option<i64>,
    /// Optional webhook push target (M16). Omit for a poll-only subscription.
    #[serde(default)]
    pub deliver: Option<DeliverBody>,
}

/// Webhook target as supplied by an operator.
///
/// The HMAC secret is referenced, never inlined: `secret_env` names an
/// environment variable of the Xpo-k process, `secret_file` a file it can read.
/// This keeps the secret out of the database, out of logs, and out of every API
/// response — an inline `secret` field is deliberately not accepted.
#[derive(Debug, Deserialize)]
pub struct DeliverBody {
    pub url: String,
    #[serde(default)]
    pub secret_env: Option<String>,
    #[serde(default)]
    pub secret_file: Option<String>,
}

/// Validate a webhook target and convert it to a storable spec.
fn parse_deliver(body: Option<DeliverBody>) -> Result<subs::DeliverySpec, String> {
    let Some(d) = body else {
        return Ok(subs::DeliverySpec::default());
    };
    let url = d.url.trim().to_string();
    if url.is_empty() {
        return Err("deliver.url must not be empty".into());
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("deliver.url must be an http(s) URL".into());
    }
    let secret_env = d
        .secret_env
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let secret_file = d
        .secret_file
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if secret_env.is_none() && secret_file.is_none() {
        // Unsigned pushes are refused outright: the receiver authenticates us
        // by HMAC, so a target without a secret can only ever be rejected.
        return Err(
            "deliver requires secret_env or secret_file (the HMAC secret is never sent inline)"
                .into(),
        );
    }
    Ok(subs::DeliverySpec {
        url: Some(url),
        secret_env,
        secret_file,
    })
}

/// Delivery view for API responses — reference only, never the secret value.
fn deliver_view(row: &subs::SubscriptionRow) -> Value {
    match row.deliver_url.as_deref() {
        None => json!({ "mode": "poll" }),
        Some(url) => json!({
            "mode": "webhook",
            "url": url,
            "secret_source": if row.deliver_secret_env.is_some() { "env" } else { "file" },
            "secret_ref": row.deliver_secret_env.clone().or_else(|| row.deliver_secret_file.clone()),
        }),
    }
}

/// `POST /subscriptions` — register interest in a session.
///
/// The cursor defaults to the session's current event seq (fetched from the
/// owning po-k), so a subscription created *before* a prompt cannot miss that
/// turn's stop and cannot fire on history. If po-k can't be reached the
/// subscription is still created, pinned at cursor 0, and the response says so
/// via `cursor_source`.
pub async fn create(State(st): State<XState>, Json(body): Json<CreateBody>) -> Resp {
    if body.session_id.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "session_id is required");
    }
    let _ = subs::sweep(&st.db, ACK_RETENTION_SECS).await;

    let subscriber = body
        .subscriber
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_string();
    if subscriber.is_empty() {
        return err(StatusCode::BAD_REQUEST, "subscriber must not be blank");
    }

    let (cursor, cursor_source) = match body.cursor {
        Some(c) => (c.max(0), "explicit"),
        None => match crate::routed::session_cursor(&st, &body.session_id).await {
            Some(c) => (c, "session"),
            None => (0, "unavailable"),
        },
    };

    let deliver = match parse_deliver(body.deliver) {
        Ok(d) => d,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };

    match subs::create_subscription(
        &st.db,
        &subscriber,
        &body.session_id,
        &body.kinds,
        &body.statuses,
        cursor,
        body.ttl_secs.unwrap_or(subs::DEFAULT_TTL_SECS),
        &deliver,
    )
    .await
    {
        Ok(row) => (
            StatusCode::CREATED,
            Json(json!({
                "subscription_id": row.id.clone(),
                "subscriber": row.subscriber.clone(),
                "session_id": row.sid.clone(),
                "kinds": if row.kinds.is_empty() { subs::DEFAULT_KINDS.iter().map(|s| s.to_string()).collect() } else { row.kinds.clone() },
                "statuses": if row.statuses.is_empty() { subs::DEFAULT_STATUSES.iter().map(|s| s.to_string()).collect() } else { row.statuses.clone() },
                "cursor": row.cursor,
                "cursor_source": cursor_source,
                "expires_at": row.expires_at,
                "deliver": deliver_view(&row),
            })),
        ),
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct UpdateBody {
    /// New webhook target, or `null`/omitted with `clear_deliver` to go
    /// poll-only.
    #[serde(default)]
    pub deliver: Option<DeliverBody>,
    #[serde(default)]
    pub clear_deliver: bool,
}

/// `PATCH /subscriptions/{id}` — change (or clear) the webhook target.
///
/// Only the delivery target is mutable; kinds/statuses/cursor are immutable by
/// design so an operator can't retroactively change what a running subscription
/// means. Re-subscribe for that.
pub async fn update(
    State(st): State<XState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateBody>,
) -> Resp {
    let spec = if body.clear_deliver {
        subs::DeliverySpec::default()
    } else {
        match parse_deliver(body.deliver) {
            Ok(d) if d.is_configured() => d,
            Ok(_) => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "provide `deliver` or set `clear_deliver: true`",
                )
            }
            Err(e) => return err(StatusCode::BAD_REQUEST, e),
        }
    };
    match subs::set_delivery(&st.db, &id, &spec).await {
        Ok(Some(row)) => {
            // A freshly pointed target should drain whatever is already queued.
            st.delivery_wake.notify_waiters();
            (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "subscription_id": row.id,
                    "deliver": deliver_view(&row),
                })),
            )
        }
        Ok(None) => err(
            StatusCode::NOT_FOUND,
            format!("subscription {id:?} not found"),
        ),
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub subscriber: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// `GET /subscriptions` — list (optionally filtered by subscriber/session).
pub async fn list(State(st): State<XState>, Query(q): Query<ListQuery>) -> Resp {
    let _ = subs::sweep(&st.db, ACK_RETENTION_SECS).await;
    match subs::list_subscriptions(&st.db, q.subscriber.as_deref(), q.session_id.as_deref()).await {
        Ok(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                let mut v = serde_json::to_value(row).unwrap_or_else(|_| json!({}));
                if let Value::Object(ref mut m) = v {
                    // Replace the raw columns with a secret-free view and add
                    // the delivery counters an operator needs for triage.
                    m.remove("deliver_url");
                    m.remove("deliver_secret_env");
                    m.remove("deliver_secret_file");
                    m.insert("deliver".into(), deliver_view(row));
                    if let Ok(summary) = subs::delivery_summary(&st.db, &row.id).await {
                        m.insert("delivery".into(), summary);
                    }
                }
                out.push(v);
            }
            (
                StatusCode::OK,
                Json(json!({ "subscriptions": out, "count": out.len() })),
            )
        }
        Err(e) => internal(e),
    }
}

/// `DELETE /subscriptions/{id}` — unsubscribe and drop its queued rows.
pub async fn delete(State(st): State<XState>, Path(id): Path<String>) -> Resp {
    match subs::delete_subscription(&st.db, &id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "subscription_id": id })),
        ),
        Ok(false) => err(
            StatusCode::NOT_FOUND,
            format!("subscription {id:?} not found"),
        ),
        Err(e) => internal(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct PollQuery {
    #[serde(default)]
    pub subscriber: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    /// Seconds to long-poll when nothing is pending (0 = return immediately).
    #[serde(default)]
    pub wait: Option<u64>,
}

/// `GET /notifications` — pending (unacked) notifications, oldest first.
///
/// Nothing is removed by reading: a notification stays pending until it is
/// acked, so a crashed or interrupted consumer sees it again.
pub async fn poll(State(st): State<XState>, Query(q): Query<PollQuery>) -> Resp {
    let subscriber = q.subscriber.as_deref();
    let sid = q.session_id.as_deref();
    let limit = q.limit.unwrap_or(20);
    let wait = q.wait.unwrap_or(0).min(MAX_POLL_WAIT);

    // Arm the waiter before the first read (see `subs::NotifyHub`).
    let waiter = subscriber.map(|s| st.notify_hub.waiter(s));
    let notified = waiter.as_ref().map(|w| w.notified());
    tokio::pin!(notified);
    if let Some(n) = notified.as_mut().as_pin_mut() {
        n.enable();
    }

    let mut rows = match subs::pending(&st.db, subscriber, sid, limit).await {
        Ok(r) => r,
        Err(e) => return internal(e),
    };
    if rows.is_empty() && wait > 0 {
        if let Some(n) = notified.as_mut().as_pin_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(wait), n).await;
        } else {
            // No subscriber filter → nothing to park on; degrade to a sleep so
            // the caller still gets long-poll-ish behaviour.
            tokio::time::sleep(Duration::from_secs(wait.min(5))).await;
        }
        rows = match subs::pending(&st.db, subscriber, sid, limit).await {
            Ok(r) => r,
            Err(e) => return internal(e),
        };
    }
    (
        StatusCode::OK,
        Json(json!({ "notifications": rows, "count": rows.len() })),
    )
}

#[derive(Debug, Deserialize)]
pub struct AckBody {
    #[serde(default)]
    pub ids: Vec<String>,
}

/// `POST /notifications/ack` — acknowledge delivery.
///
/// Idempotent: ids that are unknown or already acked are counted in `already`
/// and never error, so a retried ack is safe.
pub async fn ack(State(st): State<XState>, Json(body): Json<AckBody>) -> Resp {
    if body.ids.is_empty() {
        return err(StatusCode::BAD_REQUEST, "ids must not be empty");
    }
    match subs::ack(&st.db, &body.ids).await {
        Ok((acked, already)) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "acked": acked, "already_acked": already })),
        ),
        Err(e) => internal(e),
    }
}
