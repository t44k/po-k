//! WebSocket server: accepts po-k connections at `/ws`, runs the registration
//! handshake, and demultiplexes inbound frames back to the HTTP handlers
//! waiting on a round-trip (via the correlation maps in [`crate::registry`]).

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use pok_proto::WsMsg;
use tokio::sync::mpsc;

use crate::registry::{PokConn, StreamFrame, WsResult};
use crate::state::XState;
use crate::store;
use crate::subs;

pub fn router(state: XState) -> Router {
    Router::new()
        .route("/ws", get(upgrade))
        .with_state(state)
}

async fn upgrade(
    State(state): State<XState>,
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    // Bearer check on the upgrade request (the /ws route is outside the HTTP
    // bearer middleware).
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer ")))
        .unwrap_or("");
    if !state.token.matches(presented) {
        return (StatusCode::UNAUTHORIZED, "invalid bearer token").into_response();
    }
    ws.on_upgrade(move |socket| handle(socket, state))
}

async fn handle(socket: WebSocket, state: XState) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<WsMsg>();

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = serde_json::to_string(&msg).unwrap_or_default();
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let mut pok_id: Option<String> = None;

    while let Some(frame) = stream.next().await {
        let Ok(frame) = frame else { break };
        let txt = match frame {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let msg: WsMsg = match serde_json::from_str(&txt) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "xpo-k: undecodable po-k frame");
                continue;
            }
        };
        inbound(&state, &tx, &mut pok_id, msg).await;
    }

    if let Some(id) = pok_id {
        tracing::info!(pok_id = %id, "po-k disconnected");
        state.registry.disconnect(&id);
    }
    writer.abort();
}

/// Match one forwarded event against the subscriptions for its session and
/// wake anyone long-polling. Failures are logged, never fatal: notification
/// delivery must not break event forwarding or session routing.
async fn deliver(state: &XState, sid: &str, event: &pok_proto::EventEnvelope) {
    match subs::match_event(
        &state.db,
        sid,
        &event.kind,
        event.seq,
        &event.ts,
        &event.payload,
    )
    .await
    {
        Ok(woken) => wake_all(state, woken),
        Err(e) => tracing::warn!(sid, error = %e, "event notification match failed"),
    }
}

fn wake_all(state: &XState, subscribers: Vec<String>) {
    if subscribers.is_empty() {
        return;
    }
    for s in &subscribers {
        state.notify_hub.wake(s);
    }
    // Something was queued — kick the webhook delivery loop so the push goes
    // out now rather than on its next idle tick (M16).
    state.delivery_wake.notify_waiters();
}

/// After a po-k (re)registers, replay the events it persisted while the uplink
/// was down into any subscription that is still waiting on one of its sessions.
/// Uniqueness on `(sub_id, seq, kind)` makes this idempotent, so replaying a
/// window we already delivered is a no-op.
fn spawn_replay(state: &XState, pok_id: &str) {
    let state = state.clone();
    let pok_id = pok_id.to_string();
    tokio::spawn(async move {
        let subs_list = match subs::all_active(&state.db).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "replay: cannot list subscriptions");
                return;
            }
        };
        for sub in subs_list {
            if state.registry.pok_for_session(&sub.sid).as_deref() != Some(pok_id.as_str()) {
                continue;
            }
            let events = crate::routed::replay_events(&state, &pok_id, &sub.sid, sub.cursor).await;
            if events.is_empty() {
                continue;
            }
            let mut woken = Vec::new();
            for ev in &events {
                let kind = ev.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let seq = ev.get("seq").and_then(|s| s.as_i64()).unwrap_or(0);
                let ts = ev.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                match subs::match_event(&state.db, &sub.sid, kind, seq, ts, ev).await {
                    Ok(w) => woken.extend(w),
                    Err(e) => tracing::warn!(sid = %sub.sid, error = %e, "replay match failed"),
                }
            }
            if !woken.is_empty() {
                tracing::info!(
                    sid = %sub.sid, pok_id = %pok_id, replayed = events.len(),
                    "replayed missed events into subscription"
                );
                wake_all(&state, woken);
            }
        }
    });
}

async fn inbound(
    state: &XState,
    tx: &mpsc::UnboundedSender<WsMsg>,
    pok_id: &mut Option<String>,
    msg: WsMsg,
) {
    let reg = &state.registry;
    match msg {
        WsMsg::Register {
            pok_id: id,
            hostname,
            version,
            projects,
            sessions,
            caps,
        } => {
            let conn = PokConn {
                pok_id: id.clone(),
                hostname: hostname.clone(),
                version,
                tx: tx.clone(),
                caps,
            };
            let session_pairs: Vec<(String, String)> = sessions
                .iter()
                .map(|s| (s.sid.clone(), s.project.clone()))
                .collect();
            match reg.register(conn, &projects, &session_pairs) {
                Ok(()) => {
                    *pok_id = Some(id.clone());
                    // Seed aggregated session rows.
                    for s in &sessions {
                        let _ = sqlx::query(
                            "INSERT OR REPLACE INTO xpok_sessions (sid, pok_id, project, status, started_at) VALUES (?1,?2,?3,?4,?5)",
                        )
                        .bind(&s.sid)
                        .bind(&id)
                        .bind(&s.project)
                        .bind(&s.status)
                        .bind(store::now_iso())
                        .execute(&state.db)
                        .await;
                    }
                    tracing::info!(pok_id = %id, hostname = %hostname, projects = projects.len(), "po-k registered");
                    let _ = tx.send(WsMsg::Registered { pok_id: id.clone() });
                    // Catch subscriptions up on anything this po-k persisted
                    // while it was disconnected.
                    spawn_replay(state, &id);
                }
                Err(existing_pok_id) => {
                    tracing::warn!(
                        pok_id = %id,
                        hostname = %hostname,
                        existing = %existing_pok_id,
                        "rejecting registration: hostname already taken"
                    );
                    let _ = tx.send(WsMsg::Error {
                        request_id: None,
                        code: pok_proto::ErrorCode::Conflict,
                        message: format!(
                            "hostname {hostname:?} already registered by po-k {existing_pok_id}"
                        ),
                    });
                }
            }
        }
        WsMsg::ConfigUpdate { projects } => {
            if let Some(id) = pok_id.as_deref() {
                reg.update_projects(id, &projects);
            }
        }
        WsMsg::WsResponse {
            request_id,
            status,
            body,
            ..
        } => {
            if let Some((_, sender)) = reg.pending.remove(&request_id) {
                let _ = sender.send(WsResult { status, body });
            }
        }
        WsMsg::WsStreamChunk { request_id, data } => {
            if let Some(s) = reg.streams.get(&request_id) {
                let _ = s.send(StreamFrame::Chunk(data));
            }
        }
        WsMsg::WsStreamEnd { request_id } => {
            if let Some((_, s)) = reg.streams.remove(&request_id) {
                let _ = s.send(StreamFrame::End);
            }
        }
        WsMsg::ProfileAck {
            request_id,
            plugin_dir,
        } => {
            if let Some((_, sender)) = reg.profile_acks.remove(&request_id) {
                let _ = sender.send(plugin_dir);
            }
        }
        WsMsg::SessionEvent { sid, event } => {
            if let Some(id) = pok_id.as_deref() {
                reg.session_to_pok.insert(sid.clone(), id.to_string());
                if event.kind == "session_end" || event.kind == "cc_exited" {
                    let _ = sqlx::query("UPDATE xpok_sessions SET ended_at = ?1, status = 'ended' WHERE sid = ?2")
                        .bind(store::now_iso())
                        .bind(&sid)
                        .execute(&state.db)
                        .await;
                }
            }
            deliver(state, &sid, &event).await;
        }
        WsMsg::StatusUpdate { sid, status } => {
            let _ = sqlx::query("UPDATE xpok_sessions SET status = ?1 WHERE sid = ?2")
                .bind(&status)
                .bind(&sid)
                .execute(&state.db)
                .await;
            match subs::match_status(&state.db, &sid, &status).await {
                Ok(woken) => wake_all(state, woken),
                Err(e) => tracing::warn!(sid, error = %e, "status notification match failed"),
            }
        }
        WsMsg::Error {
            request_id: Some(rid),
            message,
            ..
        } => {
            {
                if let Some((_, sender)) = reg.pending.remove(&rid) {
                    let _ = sender.send(WsResult {
                        status: 502,
                        body: serde_json::json!({ "error": message }).to_string(),
                    });
                } else if let Some((_, s)) = reg.streams.remove(&rid) {
                    let _ = s.send(StreamFrame::Error(message));
                } else if let Some((_, s)) = reg.profile_acks.remove(&rid) {
                    drop(s); // resolves to "channel closed" on the waiter
                }
            }
        }
        _ => {}
    }
}
