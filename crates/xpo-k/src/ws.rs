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
        } => {
            let conn = PokConn {
                pok_id: id.clone(),
                hostname,
                version,
                tx: tx.clone(),
            };
            let session_pairs: Vec<(String, String)> = sessions
                .iter()
                .map(|s| (s.sid.clone(), s.project.clone()))
                .collect();
            reg.register(conn, &projects, &session_pairs);
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
            tracing::info!(pok_id = %id, projects = projects.len(), "po-k registered");
            let _ = tx.send(WsMsg::Registered { pok_id: id });
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
        }
        WsMsg::StatusUpdate { sid, status } => {
            let _ = sqlx::query("UPDATE xpok_sessions SET status = ?1 WHERE sid = ?2")
                .bind(&status)
                .bind(&sid)
                .execute(&state.db)
                .await;
        }
        WsMsg::Error {
            request_id,
            message,
            ..
        } => {
            if let Some(rid) = request_id {
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
