//! SSE live tap at `GET /api/stream?session_key=…`.
//!
//! Subscribes to the per-session EventBus that `/ingest` already publishes onto.
//! Each new event becomes one SSE frame; a heartbeat ticker every 15s keeps the
//! connection healthy through middleboxes. v1 supports per-session subscription
//! (one stream per session); a project-wide "subscribe to all sessions in this
//! project" mode is a future extension.

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use futures::stream::{Stream, StreamExt};
use po_k_proto::HEADER_API_KEY;
use serde::Deserialize;
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

use crate::auth;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct StreamQuery {
    /// Required: which session to subscribe to.
    pub session_key: Option<String>,
}

pub async fn stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<StreamQuery>,
) -> Response {
    let key = headers
        .get(HEADER_API_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let ctx = match auth::lookup(state.pool(), key).await {
        Ok(Some(ctx)) => ctx,
        Ok(None) => return (StatusCode::UNAUTHORIZED, "invalid or missing api key").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stream auth lookup");
            return (StatusCode::INTERNAL_SERVER_ERROR, "auth lookup failed").into_response();
        }
    };
    let Some(session_key) = q.session_key.filter(|s| !s.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            "missing ?session_key=... (v1 supports per-session streams only)",
        )
            .into_response();
    };

    // Verify the session belongs to the caller's team (so a key can't snoop on another
    // team's session). Sidechain events go to the same bus, so this single check covers
    // both main and subagent traffic.
    let owner_team: Option<String> =
        sqlx::query_scalar("SELECT team_id FROM sessions WHERE session_key = ?")
            .bind(&session_key)
            .fetch_optional(state.pool())
            .await
            .ok()
            .flatten();
    match owner_team.as_deref() {
        Some(t) if t == ctx.team_id => {}
        Some(_) => {
            return (StatusCode::FORBIDDEN, "session belongs to a different team").into_response();
        }
        None => {
            return (StatusCode::NOT_FOUND, "session not found").into_response();
        }
    }

    let bus_rx = state.bus().subscribe(&session_key);

    // Each new HTML snippet from the bus becomes one SSE frame, tagged with the
    // session_key for clients that multiplex.
    let session_for_frame = session_key.clone();
    let event_stream = BroadcastStream::new(bus_rx).map(move |item| -> Result<Event, Infallible> {
        match item {
            Ok(html) => Ok(Event::default()
                .event("session.event")
                .data(html_to_json_payload(&session_for_frame, &html))),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!(session = %session_for_frame, lagged = n, "sse stream lagged");
                Ok(Event::default()
                    .event("session.lag")
                    .data(format!("{{\"missed\":{n}}}")))
            }
        }
    });

    // Periodic live-status frame so consumers can watch "is CC working".
    let pool = state.pool().clone();
    let session_for_live = session_key.clone();
    let live_stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(5)))
        .skip(1) // tokio's interval fires immediately; let the bus frame race ahead
        .then(move |_| {
            let pool = pool.clone();
            let sk = session_for_live.clone();
            async move {
                let row = sqlx::query(
                    "SELECT status, active_subagents, background_tasks, updated_at, heartbeat_at
                     FROM live_sessions WHERE session_key = ?",
                )
                .bind(&sk)
                .fetch_optional(&pool)
                .await
                .ok()
                .flatten();
                let json = match row {
                    Some(r) => {
                        use sqlx::Row;
                        let status: String = r.try_get("status").unwrap_or_default();
                        let subs: i64 = r.try_get("active_subagents").unwrap_or(0);
                        let bg: i64 = r.try_get("background_tasks").unwrap_or(0);
                        let updated: Option<String> = r.try_get("updated_at").ok();
                        let heartbeat: Option<String> = r.try_get("heartbeat_at").ok();
                        serde_json::json!({
                            "session_key": sk,
                            "status": status,
                            "active_subagents": subs,
                            "background_tasks": bg,
                            "updated_at": updated,
                            "heartbeat_at": heartbeat,
                        })
                    }
                    None => serde_json::json!({"session_key": sk, "status": "unknown"}),
                };
                Ok::<Event, Infallible>(Event::default().event("session.live").data(json.to_string()))
            }
        });

    let merged = futures::stream::select(event_stream, live_stream);
    Sse::new(merged)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("ping"))
        .into_response()
}

fn html_to_json_payload(session_key: &str, html: &str) -> String {
    // Keep the SSE frame self-describing — consumers may want the html-rendered
    // turn (what the WebSocket transcript ships) as well as the raw session_key.
    serde_json::json!({
        "session_key": session_key,
        "html": html,
    })
    .to_string()
}
