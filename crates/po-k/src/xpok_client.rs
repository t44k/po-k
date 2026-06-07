//! WebSocket client to Xpo-k (M14 §5.4). po-k initiates and maintains a single
//! persistent connection: it registers, serves routed `ws_request`s by handing
//! them to [`crate::ws_dispatcher`], streams `ws_stream_chunk`s for SSE
//! endpoints, applies pushed profiles, and proactively forwards events via the
//! uplink installed in [`AppState`]. Auto-reconnects with full re-registration.

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use pok_proto::{ProjectDecl, SessionDecl, WsMsg};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::config::Xpok;
use crate::state::AppState;
use crate::ws_dispatcher::{self, Dispatched};

/// Spawn the client loop. Returns immediately; reconnects forever.
pub fn spawn(state: AppState, cfg: Xpok) {
    tokio::spawn(async move {
        let interval = cfg.reconnect_interval.0;
        loop {
            if let Err(e) = connect_once(&state, &cfg).await {
                tracing::warn!(error = %e, "xpok connection ended; reconnecting");
            }
            // Clear the uplink so events stop trying to use a dead socket.
            *state.uplink.lock().await = None;
            tokio::time::sleep(interval).await;
        }
    });
}

fn stable_pok_id(state: &AppState) -> String {
    let host = resolve_hostname(state);
    let path = state.config_path.to_string_lossy();
    // Simple stable hash of hostname + config path.
    let mut h: u64 = 1469598103934665603;
    for b in format!("{host}:{path}").bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("{host}-{h:x}")
}

/// Return the hostname to advertise to Xpo-k. Prefers the explicit
/// `xpok.hostname` config value; falls back to the OS hostname.
fn resolve_hostname(state: &AppState) -> String {
    // config is behind an async RwLock but we need this in a sync context
    // (stable_pok_id). Try_read is fine during startup — the lock is never
    // held long.
    if let Ok(cfg) = state.config.try_read() {
        if let Some(ref xpok) = cfg.xpok {
            if let Some(ref h) = xpok.hostname {
                if !h.is_empty() {
                    return h.clone();
                }
            }
        }
    }
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".into())
}

async fn connect_once(state: &AppState, cfg: &Xpok) -> Result<()> {
    let mut req = cfg
        .url
        .as_str()
        .into_client_request()
        .with_context(|| format!("bad xpok url {:?}", cfg.url))?;
    if !cfg.token.is_empty() {
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {}", cfg.token)
                .parse()
                .context("token header")?,
        );
    }
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .with_context(|| format!("connecting to {}", cfg.url))?;
    tracing::info!(url = %cfg.url, "connected to xpo-k");
    let (mut sink, mut stream) = ws.split();

    // Outbound channel: the single writer to the socket. The uplink shares it.
    let (tx, mut rx) = mpsc::unbounded_channel::<WsMsg>();
    *state.uplink.lock().await = Some(tx.clone());

    // Registration.
    tx.send(register_msg(state).await).ok();

    // Per-request cancel handles for in-flight streams.
    let cancels: Arc<DashMap<Uuid, mpsc::UnboundedSender<()>>> = Arc::new(DashMap::new());

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = serde_json::to_string(&msg).unwrap_or_default();
            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    while let Some(frame) = stream.next().await {
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "xpok read error");
                break;
            }
        };
        match frame {
            Message::Text(txt) => {
                let msg: WsMsg = match serde_json::from_str(&txt) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, "xpok: undecodable frame");
                        continue;
                    }
                };
                handle_inbound(state, &tx, &cancels, msg);
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => {
                tracing::info!("xpok closed connection");
                break;
            }
            _ => {}
        }
    }

    writer.abort();
    Ok(())
}

fn handle_inbound(
    state: &AppState,
    tx: &mpsc::UnboundedSender<WsMsg>,
    cancels: &Arc<DashMap<Uuid, mpsc::UnboundedSender<()>>>,
    msg: WsMsg,
) {
    match msg {
        WsMsg::Registered { pok_id } => {
            tracing::info!(pok_id, "registered with xpo-k");
        }
        WsMsg::WsRequest {
            request_id,
            method,
            path,
            body,
            stream,
            ..
        } => {
            if stream {
                serve_stream(state, tx, cancels, request_id, method, path, body);
            } else {
                serve_unary(state, tx, request_id, method, path, body);
            }
        }
        WsMsg::WsCancel { request_id } => {
            if let Some((_, c)) = cancels.remove(&request_id) {
                let _ = c.send(());
            }
        }
        WsMsg::PushProfile {
            request_id,
            session_id,
            profile,
        } => {
            serve_push_profile(state, tx, request_id, session_id, profile);
        }
        WsMsg::ProfileUpdate {
            session_id,
            profile,
            changed_fields,
        } => {
            let state = state.clone();
            tokio::spawn(async move {
                crate::live_reload::apply(&state, &session_id, profile, changed_fields).await;
            });
        }
        WsMsg::Error { message, .. } => {
            tracing::warn!(message, "xpok error frame");
        }
        _ => {}
    }
}

fn serve_unary(
    state: &AppState,
    tx: &mpsc::UnboundedSender<WsMsg>,
    request_id: Uuid,
    method: String,
    path: String,
    body: Option<String>,
) {
    let state = state.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        let resp = ws_dispatcher::dispatch(&state, &method, &path, body.as_deref()).await;
        let msg = match resp {
            Dispatched::Unary { status, body } => WsMsg::WsResponse {
                request_id,
                status,
                headers: Default::default(),
                body,
            },
            // A stream route reached over a non-stream request: drain to a body.
            Dispatched::Stream(_) => WsMsg::WsResponse {
                request_id,
                status: 400,
                headers: Default::default(),
                body: r#"{"error":"endpoint requires stream:true"}"#.into(),
            },
        };
        let _ = tx.send(msg);
    });
}

fn serve_stream(
    state: &AppState,
    tx: &mpsc::UnboundedSender<WsMsg>,
    cancels: &Arc<DashMap<Uuid, mpsc::UnboundedSender<()>>>,
    request_id: Uuid,
    method: String,
    path: String,
    body: Option<String>,
) {
    let state = state.clone();
    let tx = tx.clone();
    let cancels = cancels.clone();
    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<()>();
    cancels.insert(request_id, cancel_tx);
    tokio::spawn(async move {
        match ws_dispatcher::dispatch(&state, &method, &path, body.as_deref()).await {
            Dispatched::Stream(mut s) => loop {
                tokio::select! {
                    _ = cancel_rx.recv() => break,
                    frame = s.next() => match frame {
                        Some(data) => {
                            if tx.send(WsMsg::WsStreamChunk { request_id, data }).is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            },
            Dispatched::Unary { status, body } => {
                // e.g. 404 before the stream starts — surface as a chunk.
                let _ = tx.send(WsMsg::WsStreamChunk {
                    request_id,
                    data: format!("event: error\ndata: {{\"status\":{status},\"body\":{body}}}\n\n"),
                });
            }
        }
        let _ = tx.send(WsMsg::WsStreamEnd { request_id });
        cancels.remove(&request_id);
    });
}

fn serve_push_profile(
    state: &AppState,
    tx: &mpsc::UnboundedSender<WsMsg>,
    request_id: Uuid,
    session_id: Option<String>,
    profile: serde_json::Value,
) {
    let state = state.clone();
    let tx = tx.clone();
    tokio::spawn(async move {
        match crate::live_reload::generate_for(&state, session_id.as_deref(), &profile).await {
            Ok(dir) => {
                let _ = tx.send(WsMsg::ProfileAck {
                    request_id,
                    plugin_dir: dir,
                });
            }
            Err(e) => {
                let _ = tx.send(WsMsg::Error {
                    request_id: Some(request_id),
                    code: pok_proto::ErrorCode::Internal,
                    message: format!("{e:#}"),
                });
            }
        }
    });
}

async fn register_msg(state: &AppState) -> WsMsg {
    let cfg = state.config.read().await;
    let projects = cfg
        .projects
        .iter()
        .map(|p| ProjectDecl {
            name: p.name.clone(),
            cwd: p.cwd.clone(),
        })
        .collect();
    let caps = pok_proto::PokCaps {
        ad_hoc: cfg.cc.ad_hoc,
    };
    let hostname_override = cfg
        .xpok
        .as_ref()
        .and_then(|x| x.hostname.clone())
        .filter(|h| !h.is_empty());
    drop(cfg);
    let sessions = state
        .sessions
        .list()
        .await
        .into_iter()
        .map(|s| SessionDecl {
            sid: s.sid,
            project: s.project,
            status: String::new(),
        })
        .collect();
    let hostname = hostname_override.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_default()
    });
    WsMsg::Register {
        pok_id: stable_pok_id(state),
        hostname,
        version: env!("CARGO_PKG_VERSION").to_string(),
        projects,
        sessions,
        caps,
    }
}
