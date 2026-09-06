//! Hub endpoints: connect/list/forget remote hosts, proxy the session API to
//! a host, and manage watches.

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

use super::body::PokJson;
use crate::defaults;
use crate::hub::store::{self, HostRow, WebhookTarget};
use crate::hub::{hosts, watcher, webhook};
use crate::state::AppState;
use crate::version;

type Resp = (StatusCode, Json<Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> Resp {
    (code, Json(json!({ "error": msg.into() })))
}

/// Where the hub POSTs notifications. The secret is referenced, never sent.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookSpec {
    /// http(s) URL of the orchestrator's webhook receiver.
    pub url: String,
    /// Name of an env var of the `po-k serve` process holding the HMAC secret.
    #[serde(default)]
    pub secret_env: Option<String>,
    /// Path of a file holding the HMAC secret (alternative to secret_env).
    #[serde(default)]
    pub secret_file: Option<String>,
}

impl From<WebhookSpec> for WebhookTarget {
    fn from(w: WebhookSpec) -> Self {
        WebhookTarget { url: w.url, secret_env: w.secret_env, secret_file: w.secret_file }
    }
}

/// `POST /hosts`
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectBody {
    /// Box name (`ange` → `ange.zrz:13658`), `host:port`, `http://host:port`, or `local`.
    pub host: String,
    /// Default webhook for watches on this host.
    #[serde(default)]
    pub webhook: Option<WebhookSpec>,
    /// Small JSON object echoed in every webhook (e.g. chat routing ids). ≤ 2 KB.
    #[serde(default)]
    pub meta: Value,
}

/// `POST /watches`
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WatchBody {
    /// A connected host key (or `local`).
    pub host: String,
    pub session_id: String,
    /// Overrides the host's default webhook.
    #[serde(default)]
    pub webhook: Option<WebhookSpec>,
    /// Overrides the host's default meta.
    #[serde(default)]
    pub meta: Value,
}

fn check_meta(meta: &Value) -> Result<(), Resp> {
    if meta.is_null() {
        return Ok(());
    }
    if !meta.is_object() {
        return Err(err(StatusCode::BAD_REQUEST, "meta must be a JSON object"));
    }
    let len = meta.to_string().len();
    if len > defaults::META_MAX_BYTES {
        return Err(err(StatusCode::BAD_REQUEST, format!("meta is {len} bytes; max {}", defaults::META_MAX_BYTES)));
    }
    Ok(())
}

fn validate_webhook(spec: Option<WebhookSpec>) -> Result<Option<WebhookTarget>, Resp> {
    match spec {
        None => Ok(None),
        Some(s) => {
            let t: WebhookTarget = s.into();
            webhook::validate_target(&t).map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
            Ok(Some(t))
        }
    }
}

fn local_row(state: &AppState) -> HostRow {
    HostRow {
        host: hosts::LOCAL.into(),
        base_url: state.config.server.callback_base_url(),
        webhook: None,
        meta: Value::Null,
        added_at: crate::events_store::now_iso(),
        last_seen_at: None,
        last_error: None,
    }
}

/// The host row, synthesising `local` when it was never explicitly connected.
async fn host_row(state: &AppState, host: &str) -> Result<HostRow, Resp> {
    match store::get_host(&state.db, host).await {
        Ok(Some(h)) => Ok(h),
        Ok(None) if host == hosts::LOCAL => Ok(local_row(state)),
        Ok(None) => Err(err(StatusCode::NOT_FOUND, format!("host {host:?} is not connected — POST /hosts first"))),
        Err(e) => Err(err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))),
    }
}

/// Why a probe failed; `Mismatch` is reported as 409, the rest as 502.
#[derive(Debug)]
enum ProbeError {
    Unreachable(String),
    Mismatch { message: String, remote_version: String },
}

impl ProbeError {
    fn message(&self) -> &str {
        match self {
            ProbeError::Unreachable(m) | ProbeError::Mismatch { message: m, .. } => m,
        }
    }
}

/// Reach a po-k: `/health` (unauthenticated; its `version` must equal ours),
/// then `/sessions` with the fleet token and our version header.
async fn probe(state: &AppState, base_url: &str) -> Result<Value, ProbeError> {
    let health = version::tag(state.hub.client.get(format!("{base_url}/health")))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| ProbeError::Unreachable(format!("cannot reach {base_url}: {e}")))?;
    let health_status = health.status().as_u16();
    let health_text = health.text().await.unwrap_or_default();
    if !(200..300).contains(&health_status) {
        return Err(ProbeError::Unreachable(format!(
            "{base_url}/health returned HTTP {health_status}: {}",
            health_text.chars().take(160).collect::<String>()
        )));
    }
    let hv: Value = serde_json::from_str(&health_text).unwrap_or(Value::Null);
    let remote_version = hv.get("version").and_then(Value::as_str).unwrap_or("").to_string();
    version::check(&remote_version, &format!("host {base_url}")).map_err(|message| ProbeError::Mismatch {
        message,
        remote_version: remote_version.clone(),
    })?;
    let sessions = version::tag(state.hub.client.get(format!("{base_url}/sessions")))
        .bearer_auth(state.token.raw())
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| ProbeError::Unreachable(format!("cannot reach {base_url}: {e}")))?;
    let status = sessions.status().as_u16();
    match status {
        401 | 403 => return Err(ProbeError::Unreachable(format!("{base_url} rejected the fleet token (HTTP {status}) — both po-ks must share one auth.token"))),
        409 => {
            let text = sessions.text().await.unwrap_or_default();
            return Err(ProbeError::Mismatch { message: format!("{base_url}: {text}"), remote_version });
        }
        s if !(200..300).contains(&s) => return Err(ProbeError::Unreachable(format!("{base_url}/sessions returned HTTP {s}"))),
        _ => {}
    }
    let list: Value = sessions.json().await.unwrap_or(json!([]));
    Ok(json!({ "version": hv.get("version").cloned().unwrap_or(Value::Null), "sessions": list }))
}

pub async fn connect(State(state): State<AppState>, PokJson(body): PokJson<ConnectBody>) -> Resp {
    let resolved = match hosts::resolve(&body.host, &state.config.server.callback_base_url()) {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    let webhook = match validate_webhook(body.webhook) {
        Ok(w) => w,
        Err(e) => return e,
    };
    if let Err(e) = check_meta(&body.meta) {
        return e;
    }
    let probe = match probe(&state, &resolved.base_url).await {
        Ok(p) => p,
        Err(ProbeError::Mismatch { message, remote_version }) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": message,
                    "host": resolved.key,
                    "base_url": resolved.base_url,
                    "local_version": version::VERSION,
                    "remote_version": remote_version,
                })),
            )
        }
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": e.message(), "host": resolved.key, "base_url": resolved.base_url })),
            )
        }
    };
    if let Err(e) = store::upsert_host(&state.db, &resolved.key, &resolved.base_url, webhook.as_ref(), &body.meta).await {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    }
    (
        StatusCode::OK,
        Json(json!({
            "host": resolved.key,
            "base_url": resolved.base_url,
            "version": probe["version"],
            "sessions": probe["sessions"],
            "webhook": webhook,
            "meta": body.meta,
        })),
    )
}

pub async fn list_hosts(State(state): State<AppState>) -> Resp {
    let rows = match store::list_hosts(&state.db).await {
        Ok(r) => r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    };
    let active = store::active_watches(&state.db).await.unwrap_or_default();
    let out: Vec<Value> = rows
        .into_iter()
        .map(|h| {
            let n = active.iter().filter(|w| w.host == h.host).count();
            let mut v = serde_json::to_value(&h).unwrap_or(json!({}));
            v["active_watches"] = json!(n);
            v
        })
        .collect();
    (StatusCode::OK, Json(json!(out)))
}

pub async fn get_host(State(state): State<AppState>, Path(host): Path<String>) -> Resp {
    let row = match host_row(&state, &host).await {
        Ok(r) => r,
        Err(e) => return e,
    };
    let probe = match probe(&state, &row.base_url).await {
        Ok(p) => {
            let _ = store::touch_host(&state.db, &row.host, None).await;
            json!({ "ok": true, "version": p["version"], "sessions": p["sessions"] })
        }
        Err(e) => {
            let _ = store::touch_host(&state.db, &row.host, Some(e.message())).await;
            json!({ "ok": false, "error": e.message(), "version_mismatch": matches!(e, ProbeError::Mismatch { .. }) })
        }
    };
    let mut v = serde_json::to_value(&row).unwrap_or(json!({}));
    v["probe"] = probe;
    (StatusCode::OK, Json(v))
}

pub async fn delete_host(State(state): State<AppState>, Path(host): Path<String>) -> Resp {
    let active = store::list_watches(&state.db, Some(&host), Some("active")).await.unwrap_or_default();
    for w in &active {
        state.hub.abort_task(&w.id).await;
        let _ = store::set_state(&state.db, &w.id, "stopped", None, Some("host disconnected")).await;
    }
    match store::delete_host(&state.db, &host).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true, "host": host, "watches_stopped": active.len() }))),
        Ok(false) => err(StatusCode::NOT_FOUND, format!("host {host:?} is not connected")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

fn proxy_timeout(method: &Method, path: &str) -> Duration {
    if path.ends_with("/wait") || path.ends_with("/events") || (path.ends_with("/messages") && method == Method::GET) {
        Duration::from_secs(660)
    } else if path.ends_with("/messages") || path.ends_with("/clear") {
        Duration::from_secs(130)
    } else if path == "/sessions" && method == Method::POST {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(30)
    }
}

/// `wake` / `webhook` / `meta` stripped from a create body, when the caller
/// asked for a watch.
struct WakeRequest {
    target: WebhookTarget,
    meta: Value,
}

fn extract_wake(body: &mut Bytes, host: &HostRow) -> Result<Option<WakeRequest>, Resp> {
    if body.is_empty() {
        return Ok(None);
    }
    let Ok(Value::Object(mut obj)) = serde_json::from_slice::<Value>(body) else {
        return Ok(None); // let the remote reject it
    };
    let wake = obj.remove("wake");
    let webhook_v = obj.remove("webhook");
    let meta_v = obj.remove("meta");
    let wants = match &wake {
        Some(Value::Bool(b)) => *b,
        None => webhook_v.is_some(),
        Some(_) => return Err(err(StatusCode::BAD_REQUEST, "wake must be a boolean")),
    };
    let out = if wants {
        let explicit: Option<WebhookSpec> = match webhook_v {
            Some(v) => Some(serde_json::from_value(v).map_err(|e| err(StatusCode::BAD_REQUEST, format!("webhook: {e}")))?),
            None => None,
        };
        let target = match validate_webhook(explicit)? {
            Some(t) => t,
            None => host.webhook.clone().ok_or_else(|| {
                err(StatusCode::BAD_REQUEST, format!("wake requested but host {:?} has no default webhook — pass `webhook` or reconnect the host with one", host.host))
            })?,
        };
        let meta = match meta_v {
            Some(m) if !m.is_null() => m,
            _ => host.meta.clone(),
        };
        check_meta(&meta)?;
        Some(WakeRequest { target, meta })
    } else {
        None
    };
    *body = Bytes::from(serde_json::to_vec(&Value::Object(obj)).unwrap_or_default());
    Ok(out)
}

/// Transparent proxy of the session API to a connected host.
pub async fn proxy(
    State(state): State<AppState>,
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    mut body: Bytes,
) -> Response {
    let Some(host) = params.get("host") else {
        return err(StatusCode::BAD_REQUEST, "missing host").into_response();
    };
    let row = match host_row(&state, host).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let rest = params.get("rest").cloned().unwrap_or_default();
    let mut path = String::from("/sessions");
    if !rest.is_empty() {
        path.push('/');
        path.push_str(&rest);
    }
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let url = format!("{}{}{}", row.base_url, path, query);
    let is_stream = path.ends_with("/stream");
    let is_create = method == Method::POST && rest.is_empty();

    let wake = if is_create {
        match extract_wake(&mut body, &row) {
            Ok(w) => w,
            Err(e) => return e.into_response(),
        }
    } else {
        None
    };

    let mut req = version::tag(state.hub.client.request(method.clone(), &url)).bearer_auth(state.token.raw());
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(v) = headers.get(&name) {
            req = req.header(name, v.clone());
        }
    }
    if !is_stream {
        req = req.timeout(proxy_timeout(&method, &path));
    }
    if !body.is_empty() {
        req = req.body(body.to_vec());
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("cannot reach host {} ({}): {e}", row.host, row.base_url);
            let _ = store::touch_host(&state.db, &row.host, Some(&msg)).await;
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": msg, "host": row.host, "base_url": row.base_url })),
            )
                .into_response();
        }
    };
    let _ = store::touch_host(&state.db, &row.host, None).await;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| header::HeaderValue::from_static("application/json"));

    if is_stream {
        let stream = resp.bytes_stream();
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => return err(StatusCode::BAD_GATEWAY, format!("reading response from {}: {e}", row.host)).into_response(),
    };
    let mut out = bytes.to_vec();
    if let (Some(w), true) = (wake, status == StatusCode::CREATED) {
        if let Ok(mut v) = serde_json::from_slice::<Value>(&bytes) {
            if let Some(sid) = v.get("session_id").and_then(Value::as_str).map(str::to_string) {
                match store::insert_watch(&state.db, &row.host, &sid, &w.target, &w.meta, 0).await {
                    Ok(watch) => {
                        watcher::spawn(&state, watch.clone());
                        v["watch"] = serde_json::to_value(&watch).unwrap_or(Value::Null);
                    }
                    Err(e) => v["watch"] = json!({ "error": format!("{e:#}") }),
                }
                out = serde_json::to_vec(&v).unwrap_or(out);
            }
        }
    }
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(out))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

pub async fn create_watch(State(state): State<AppState>, PokJson(body): PokJson<WatchBody>) -> Resp {
    let row = match host_row(&state, &body.host).await {
        Ok(r) => r,
        Err(e) => return e,
    };
    let target = match validate_webhook(body.webhook) {
        Ok(Some(t)) => t,
        Ok(None) => match row.webhook.clone() {
            Some(t) => t,
            None => return err(StatusCode::BAD_REQUEST, format!("host {:?} has no default webhook — pass `webhook`", row.host)),
        },
        Err(e) => return e,
    };
    let meta = if body.meta.is_null() { row.meta.clone() } else { body.meta };
    if let Err(e) = check_meta(&meta) {
        return e;
    }
    if let Ok(Some(existing)) = store::find_active_watch(&state.db, &row.host, &body.session_id).await {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "session is already watched", "watch_id": existing.id })),
        );
    }
    // Start at the session's current boundary so a stale stop does not fire.
    let url = format!("{}/sessions/{}/status", row.base_url, body.session_id);
    let since = match version::tag(state.hub.client.get(&url)).bearer_auth(state.token.raw()).timeout(Duration::from_secs(10)).send().await {
        Err(e) => return err(StatusCode::BAD_GATEWAY, format!("cannot reach host {} ({}): {e}", row.host, row.base_url)),
        Ok(r) if r.status().as_u16() == 404 => return err(StatusCode::NOT_FOUND, format!("session {} not found on host {}", body.session_id, row.host)),
        Ok(r) if r.status().as_u16() == 409 => {
            let text = r.text().await.unwrap_or_default();
            return err(StatusCode::CONFLICT, format!("host {}: {}", row.host, serde_json::from_str::<Value>(&text).ok().and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string)).unwrap_or(text)));
        }
        Ok(r) if !r.status().is_success() => return err(StatusCode::BAD_GATEWAY, format!("host {} returned HTTP {} for /status", row.host, r.status().as_u16())),
        Ok(r) => r.json::<Value>().await.ok().and_then(|v| v.get("boundary_cursor").and_then(Value::as_i64)).unwrap_or(0),
    };
    match store::insert_watch(&state.db, &row.host, &body.session_id, &target, &meta, since).await {
        Ok(watch) => {
            watcher::spawn(&state, watch.clone());
            (StatusCode::CREATED, Json(serde_json::to_value(&watch).unwrap_or(json!({}))))
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

pub async fn list_watches(State(state): State<AppState>, Query(q): Query<HashMap<String, String>>) -> Resp {
    match store::list_watches(&state.db, q.get("host").map(String::as_str), q.get("state").map(String::as_str)).await {
        Ok(rows) => (StatusCode::OK, Json(serde_json::to_value(&rows).unwrap_or(json!([])))),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

pub async fn get_watch(State(state): State<AppState>, Path(id): Path<String>) -> Resp {
    match store::get_watch(&state.db, &id).await {
        Ok(Some(w)) => {
            let mut v = serde_json::to_value(&w).unwrap_or(json!({}));
            v["task_running"] = json!(state.hub.running_task_ids().await.contains(&w.id));
            (StatusCode::OK, Json(v))
        }
        Ok(None) => err(StatusCode::NOT_FOUND, format!("watch {id:?} not found")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

pub async fn delete_watch(State(state): State<AppState>, Path(id): Path<String>) -> Resp {
    match store::get_watch(&state.db, &id).await {
        Ok(Some(_)) => {
            state.hub.abort_task(&id).await;
            let _ = store::set_state(&state.db, &id, "stopped", None, None).await;
            (StatusCode::OK, Json(json!({ "ok": true, "watch_id": id })))
        }
        Ok(None) => err(StatusCode::NOT_FOUND, format!("watch {id:?} not found")),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::test_support::*;

    #[test]
    fn proxy_timeouts_by_route() {
        assert_eq!(proxy_timeout(&Method::GET, "/sessions/x/wait"), Duration::from_secs(660));
        assert_eq!(proxy_timeout(&Method::GET, "/sessions/x/messages"), Duration::from_secs(660));
        assert_eq!(proxy_timeout(&Method::POST, "/sessions/x/messages"), Duration::from_secs(130));
        assert_eq!(proxy_timeout(&Method::POST, "/sessions"), Duration::from_secs(60));
        assert_eq!(proxy_timeout(&Method::DELETE, "/sessions/x"), Duration::from_secs(30));
    }

    #[test]
    fn extract_wake_strips_hub_fields_and_needs_a_webhook() {
        let mut host = HostRow {
            host: "box".into(),
            base_url: "http://box:1".into(),
            webhook: None,
            meta: Value::Null,
            added_at: "t".into(),
            last_seen_at: None,
            last_error: None,
        };
        // No wake → body untouched, no watch.
        let mut b = Bytes::from(r#"{"cwd":"/w"}"#);
        assert!(extract_wake(&mut b, &host).unwrap().is_none());
        // wake without any webhook → 400.
        let mut b = Bytes::from(r#"{"cwd":"/w","wake":true}"#);
        assert!(extract_wake(&mut b, &host).is_err());
        // Explicit webhook wins; fields are stripped from the forwarded body.
        let mut b = Bytes::from(r#"{"cwd":"/w","wake":true,"webhook":{"url":"http://h/w","secret_env":"S"},"meta":{"k":"v"}}"#);
        let w = extract_wake(&mut b, &host).unwrap().unwrap();
        assert_eq!(w.target.url, "http://h/w");
        assert_eq!(w.meta["k"], "v");
        let forwarded: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(forwarded, serde_json::json!({ "cwd": "/w" }));
        // Host default webhook + meta apply when only wake=true is given.
        host.webhook = Some(WebhookTarget { url: "http://h/d".into(), secret_env: Some("S".into()), secret_file: None });
        host.meta = serde_json::json!({ "chat": 1 });
        let mut b = Bytes::from(r#"{"cwd":"/w","wake":true}"#);
        let w = extract_wake(&mut b, &host).unwrap().unwrap();
        assert_eq!(w.target.url, "http://h/d");
        assert_eq!(w.meta["chat"], 1);
        // `webhook` alone implies wake.
        let mut b = Bytes::from(r#"{"cwd":"/w","webhook":{"url":"http://h/x","secret_file":"/s"}}"#);
        assert!(extract_wake(&mut b, &host).unwrap().is_some());
    }

    #[tokio::test]
    async fn unknown_host_is_404_and_local_is_implicit() {
        let st = test_state().await;
        let app = crate::http::router(st);
        let (s, v) = get(app.clone(), "/hosts/nope/sessions").await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert!(v["error"].as_str().unwrap().contains("POST /hosts"));
        // `local` resolves without connect; nothing listens on the test bind, so 502.
        let (s, v) = get(app, "/hosts/local/sessions").await;
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(v["error"].as_str().unwrap().contains("cannot reach"));
    }

    #[tokio::test]
    async fn connect_validates_input_before_probing() {
        let app = crate::http::router(test_state().await);
        let (s, v) = call(app.clone(), "POST", "/hosts", Some(r#"{"host":"a/b"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("host"));
        let (s, v) = call(app.clone(), "POST", "/hosts", Some(r#"{"host":"box","webhook":{"url":"http://h/w"}}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("secret"));
        let (s, v) = call(app.clone(), "POST", "/hosts", Some(r#"{"host":"box","meta":"nope"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("meta"));
        // Unreachable box → 502 with the resolved base_url.
        let (s, v) = call(app, "POST", "/hosts", Some(r#"{"host":"127.0.0.1:1"}"#)).await;
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert_eq!(v["base_url"], "http://127.0.0.1:1");
    }

    #[tokio::test]
    async fn watches_crud_without_a_remote() {
        let app = crate::http::router(test_state().await);
        let (s, v) = get(app.clone(), "/watches").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v, serde_json::json!([]));
        let (s, _) = get(app.clone(), "/watches/w-nope").await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        let (s, v) = call(app.clone(), "POST", "/watches", Some(r#"{"host":"nope","session_id":"x"}"#)).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert!(v["error"].as_str().unwrap().contains("not connected"));
        let (s, v) = call(app, "POST", "/watches", Some(r#"{"host":"local","session_id":"x"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("webhook"));
    }
}
