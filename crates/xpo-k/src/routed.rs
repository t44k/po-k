//! Orchestrator-facing endpoints that mirror po-k's HTTP API. Each call is
//! fulfilled by translating it into a `ws_request` to the owning po-k and
//! awaiting the `ws_response` (or streaming `ws_stream_chunk`s for SSE).

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::registry::{StreamFrame, WsResult};
use crate::state::XState;

const CALL_TIMEOUT: Duration = Duration::from_secs(630);

pub fn router() -> Router<XState> {
    Router::new()
        .route("/projects", get(list_projects))
        .route("/sessions", post(create_session).get(list_sessions))
        .route("/sessions/{id}", get(by_session).delete(by_session))
        .route("/sessions/{id}/messages", post(by_session).get(by_session))
        .route("/sessions/{id}/messages/stream", get(stream_session))
        .route("/sessions/{id}/interrupt", post(by_session))
        .route("/sessions/{id}/clear", post(by_session))
        .route("/sessions/{id}/files", post(by_session))
        .route("/sessions/{id}/events", get(by_session))
        .route("/sessions/{id}/events/stream", get(stream_session))
        .route("/sessions/{id}/cost", get(by_session))
        .route("/sessions/{id}/status", get(by_session))
        .route("/sessions/{id}/wait", get(by_session))
        .route("/sessions/{id}/pane", get(by_session))
        .route("/sessions/{id}/capabilities", get(by_session))
        .route(
            "/sessions/{id}/permission_requests/{req_id}",
            post(by_session),
        )
}

fn err(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, Json(json!({ "error": msg.into() }))).into_response()
}

/// Send a unary `ws_request` to a po-k and await its `ws_response`.
async fn call_pok(
    st: &XState,
    pok_id: &str,
    method: &str,
    path: &str,
    body: Option<String>,
) -> Result<WsResult, Response> {
    let request_id = Uuid::new_v4();
    let (txr, rxr) = oneshot::channel();
    st.registry.pending.insert(request_id, txr);
    let sent = st.registry.send(
        pok_id,
        pok_proto::WsMsg::WsRequest {
            request_id,
            method: method.to_string(),
            path: path.to_string(),
            headers: Default::default(),
            body,
            stream: false,
        },
    );
    if !sent {
        st.registry.pending.remove(&request_id);
        return Err(err(StatusCode::BAD_GATEWAY, "owning po-k not connected"));
    }
    match tokio::time::timeout(CALL_TIMEOUT, rxr).await {
        Ok(Ok(r)) => Ok(r),
        _ => {
            st.registry.pending.remove(&request_id);
            Err(err(StatusCode::GATEWAY_TIMEOUT, "po-k did not respond"))
        }
    }
}

fn to_response(r: WsResult) -> Response {
    let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK);
    let body: Value = serde_json::from_str(&r.body).unwrap_or(json!({ "raw": r.body }));
    (status, Json(body)).into_response()
}

/// Generic session-scoped passthrough: route by session id, forward the exact
/// path+query+body verbatim.
async fn by_session(
    State(st): State<XState>,
    Path(params): Path<std::collections::HashMap<String, String>>,
    method: axum::http::Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let Some(sid) = params.get("id") else {
        return err(StatusCode::BAD_REQUEST, "missing session id");
    };
    let Some(pok_id) = st.registry.pok_for_session(sid) else {
        return err(StatusCode::NOT_FOUND, format!("no po-k owns session {sid}"));
    };
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or(uri.path());
    let body = (!body.is_empty()).then(|| String::from_utf8_lossy(&body).into_owned());
    match call_pok(&st, &pok_id, method.as_str(), path, body).await {
        Ok(r) => to_response(r),
        Err(e) => e,
    }
}

/// `GET /projects` — fan out to every connected po-k, merge the arrays.
async fn list_projects(State(st): State<XState>) -> Response {
    fan_out(&st, "/projects").await
}

/// `GET /sessions` — fan out, merge.
async fn list_sessions(State(st): State<XState>) -> Response {
    fan_out(&st, "/sessions").await
}

async fn fan_out(st: &XState, path: &str) -> Response {
    let pok_ids: Vec<String> = st.registry.conns.iter().map(|e| e.pok_id.clone()).collect();
    let mut merged: Vec<Value> = Vec::new();
    for id in pok_ids {
        if let Ok(r) = call_pok(st, &id, "GET", path, None).await {
            if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(&r.body) {
                merged.extend(arr);
            }
        }
    }
    (StatusCode::OK, Json(json!(merged))).into_response()
}

/// Extended `POST /sessions` (spec §4.3): resolve + merge profiles, then create
/// the session on the owning po-k with the merged profile inline.
async fn create_session(State(st): State<XState>, Json(body): Json<Value>) -> Response {
    let project = match body.get("project").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return err(StatusCode::BAD_REQUEST, "missing `project`"),
    };
    let Some(pok_id) = st.registry.pok_for_project(&project) else {
        return err(
            StatusCode::NOT_FOUND,
            format!("no connected po-k owns project {project:?}"),
        );
    };

    let requested: Vec<String> = body
        .get("profiles")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let names = crate::http::profiles::resolve_names(&st, &requested, Some(&project));

    // Build the create body forwarded to po-k.
    let mut create = json!({ "project": project, "profiles": names });
    if !names.is_empty() {
        match merge_named(&st, &names).await {
            Ok(merged) => {
                create["profile"] = merged;
            }
            Err(e) => return e,
        }
    }
    for k in ["agent", "bare", "model", "effort", "cc_flags"] {
        if let Some(v) = body.get(k) {
            // cc_flags.model / cc_flags.effort take precedence.
            if k == "cc_flags" {
                if let Some(m) = v.get("model") {
                    create["model"] = m.clone();
                }
                if let Some(e) = v.get("effort") {
                    create["effort"] = e.clone();
                }
            } else {
                create[k] = v.clone();
            }
        }
    }

    match call_pok(&st, &pok_id, "POST", "/sessions", Some(create.to_string())).await {
        Ok(r) => {
            // Record the session → po-k mapping eagerly from the response.
            if let Ok(v) = serde_json::from_str::<Value>(&r.body) {
                if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
                    st.registry.session_to_pok.insert(sid.to_string(), pok_id.clone());
                    let _ = sqlx::query(
                        "INSERT OR REPLACE INTO xpok_sessions (sid, pok_id, project, profiles, started_at) VALUES (?1,?2,?3,?4,?5)",
                    )
                    .bind(sid)
                    .bind(&pok_id)
                    .bind(&project)
                    .bind(serde_json::to_string(&names).ok())
                    .bind(crate::store::now_iso())
                    .execute(&st.db)
                    .await;
                }
            }
            to_response(r)
        }
        Err(e) => e,
    }
}

async fn merge_named(st: &XState, names: &[String]) -> Result<Value, Response> {
    let mut profiles = Vec::with_capacity(names.len());
    for n in names {
        match crate::store::get_profile(&st.db, n).await {
            Ok(Some(row)) => {
                let v: Value = serde_json::from_str(&row.data)
                    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let p = pok_proto::Profile::from_json(&v)
                    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e))?;
                profiles.push(p);
            }
            Ok(None) => return Err(err(StatusCode::NOT_FOUND, format!("profile {n:?} not found"))),
            Err(e) => return Err(err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        }
    }
    let merged = crate::merge::merge(&profiles);
    serde_json::to_value(&merged).map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// SSE bridge: open a `stream:true` ws_request and relay `ws_stream_chunk`s as
/// SSE events. Cancels the po-k stream when the orchestrator disconnects.
async fn stream_session(
    State(st): State<XState>,
    Path(params): Path<std::collections::HashMap<String, String>>,
    OriginalUri(uri): OriginalUri,
    _headers: HeaderMap,
) -> Response {
    let Some(sid) = params.get("id") else {
        return err(StatusCode::BAD_REQUEST, "missing session id");
    };
    let Some(pok_id) = st.registry.pok_for_session(sid) else {
        return err(StatusCode::NOT_FOUND, format!("no po-k owns session {sid}"));
    };
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or(uri.path()).to_string();

    let request_id = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<StreamFrame>();
    st.registry.streams.insert(request_id, tx);
    let sent = st.registry.send(
        &pok_id,
        pok_proto::WsMsg::WsRequest {
            request_id,
            method: "GET".into(),
            path,
            headers: Default::default(),
            body: None,
            stream: true,
        },
    );
    if !sent {
        st.registry.streams.remove(&request_id);
        return err(StatusCode::BAD_GATEWAY, "owning po-k not connected");
    }

    // Guard fires ws_cancel + cleans up when the response body is dropped
    // (orchestrator disconnect).
    let guard = CancelGuard {
        registry: st.registry.clone(),
        pok_id,
        request_id,
    };

    // po-k already SSE-frames each chunk, so forward the bytes verbatim. The
    // guard is moved into the stream so it drops with the response body.
    let body = Body::from_stream(async_stream_relay(rx, guard));
    Response::builder()
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .unwrap()
        .into_response()
}

fn async_stream_relay(
    mut rx: mpsc::UnboundedReceiver<StreamFrame>,
    guard: CancelGuard,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        let _guard = guard; // held until the stream is dropped
        while let Some(frame) = rx.recv().await {
            match frame {
                StreamFrame::Chunk(data) => yield Ok(Bytes::from(data)),
                StreamFrame::Error(msg) => {
                    yield Ok(Bytes::from(format!("event: error\ndata: {msg}\n\n")));
                    break;
                }
                StreamFrame::End => break,
            }
        }
    }
}

struct CancelGuard {
    registry: crate::registry::Registry,
    pok_id: String,
    request_id: Uuid,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.registry.streams.remove(&self.request_id);
        self.registry.send(
            &self.pok_id,
            pok_proto::WsMsg::WsCancel {
                request_id: self.request_id,
            },
        );
    }
}
