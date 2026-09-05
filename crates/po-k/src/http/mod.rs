//! HTTP router assembly.
//!
//! `/health`, `/help` and `/docs` are public; everything else — including
//! CC's own hook + permission callbacks, which already send the bearer — is
//! behind `require_bearer`. The route list lives in [`routes::ROUTES`], which
//! also feeds `GET /docs`, so the two cannot drift.

use axum::http::{Method, StatusCode, Uri};
use axum::middleware;
use axum::routing::MethodRouter;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::auth::require_bearer;
use crate::core::{CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

pub mod body;
pub mod control;
pub mod docs;
pub mod events;
pub mod health;
pub mod help;
pub mod hooks_in;
pub mod hub;
pub mod messages;
pub mod perms;
pub mod query;
pub mod routes;
pub mod sessions;

/// Adapt a core result into `(StatusCode, Json)`, preserving the core-assigned
/// status (200/201/4xx/5xx) and JSON body verbatim.
pub(crate) fn adapt(r: CoreResult<CoreResponse>) -> (StatusCode, Json<Value>) {
    match r {
        Ok(ok) => (StatusCode::from_u16(ok.status).unwrap_or(StatusCode::OK), Json(ok.body)),
        Err(e) => adapt_err(e),
    }
}

pub(crate) fn adapt_err(e: CoreError) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(e.body()),
    )
}

pub fn router(state: AppState) -> Router {
    // Group the table by path so several methods on one path become one
    // MethodRouter, then split public vs protected.
    let mut grouped: Vec<(&'static str, bool, Option<MethodRouter<AppState>>)> = Vec::new();
    for r in routes::ROUTES {
        match grouped.iter_mut().find(|(p, _, _)| *p == r.path) {
            Some(g) => {
                assert_eq!(g.1, r.auth, "route {} mixes public and protected methods", r.path);
                let prev = g.2.take().expect("method router present");
                g.2 = Some(prev.merge((r.mount)()));
            }
            None => grouped.push((r.path, r.auth, Some((r.mount)()))),
        }
    }
    let mut public: Router<AppState> = Router::new();
    let mut protected: Router<AppState> = Router::new();
    for (path, auth, mr) in grouped {
        let mr = mr.expect("method router present");
        if auth {
            protected = protected.route(path, mr);
        } else {
            public = public.route(path, mr);
        }
    }
    let protected = protected.route_layer(middleware::from_fn_with_state(state.token.clone(), require_bearer));
    public.merge(protected).fallback(not_found).with_state(state)
}

/// JSON 404 for unmatched routes (axum's default is an empty body, which is
/// indistinguishable on the client from "endpoint returned nothing").
async fn not_found(method: Method, uri: Uri) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("no route for {method} {}", uri.path()),
            "hint": "GET /docs (JSON) or GET /help (markdown) list every endpoint",
        })),
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::auth::Token;
    use crate::config::Config;
    use axum::body::Body;
    use axum::http::{header, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    pub const TOKEN: &str = "t";

    pub async fn test_state() -> AppState {
        let db_path = std::env::temp_dir().join(format!("po-k-http-test-{}.db", uuid::Uuid::new_v4()));
        let db = crate::events_store::open(&db_path).await.unwrap();
        AppState::new(Token::__test_new(TOKEN.into()), Config::default(), db)
    }

    pub async fn seed(state: &AppState, sid: &str) {
        crate::events_store::insert_session(
            &state.db,
            &crate::events_store::SessionRow {
                sid: sid.into(),
                name: "po-k".into(),
                cwd: "/workspace".into(),
                zellij_session: "po-k-po-k".into(),
                model: None,
                effort: None,
                started_at: "2026-05-27T00:00:00Z".into(),
                ended_at: None,
                pid: None,
                last_event_seq: 0,
                plugin_dir: None,
                plugins: None,
                mcp_servers: None,
                permission_mode: None,
                agent: None,
            },
        )
        .await
        .unwrap();
    }

    pub async fn body_json(resp: axum::http::Response<Body>) -> Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        if bytes.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }))
    }

    /// Authenticated request; returns (status, json body).
    pub async fn call(app: Router, method: &str, uri: &str, body: Option<&str>) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        if body.is_some() {
            req = req.header(header::CONTENT_TYPE, "application/json");
        }
        let req = req.body(body.map(|b| Body::from(b.to_string())).unwrap_or_else(Body::empty)).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        (status, body_json(resp).await)
    }

    pub async fn get(app: Router, uri: &str) -> (StatusCode, Value) {
        call(app, "GET", uri, None).await
    }

    pub async fn append(st: &AppState, sid: &str, kind: &str) -> i64 {
        crate::events_store::append_event(&st.db, sid, "t", kind, &json!({})).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request};
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_help_docs_are_public() {
        let app = router(test_state().await);
        for uri in ["/health", "/help", "/docs"] {
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        }
    }

    #[tokio::test]
    async fn protected_requires_bearer_with_json_401() {
        let app = router(test_state().await);
        let resp = app
            .clone()
            .oneshot(Request::builder().uri("/sessions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let v = body_json(resp).await;
        assert!(v["error"].as_str().unwrap().contains("Authorization"), "{v}");

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/sessions")
                    .header(header::AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(resp).await["error"], "invalid bearer token");
    }

    #[tokio::test]
    async fn hooks_ingest_requires_bearer_and_accepts_lowercase_prefix() {
        let st = test_state().await;
        seed(&st, "h1").await;
        st.sessions
            .insert(crate::session::RunningSession {
                sid: "h1".into(),
                name: "po-k".into(),
                cwd: "/workspace".into(),
                zellij_session: "po-k-po-k".into(),
                model: "m".into(),
                effort: "e".into(),
                permission_mode: "bypassPermissions".into(),
                agent: None,
                plugins: vec![],
                mcp_servers: vec![],
                started_at: "t".into(),
                hooks_path: "/h".into(),
                mcp_path: "/m".into(),
                pid: None,
            })
            .await;
        let app = router(st);
        // No header → 401.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/h1/hooks/Stop")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        // The generated curl sends `authorization: bearer <t>` (lowercase).
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/h1/hooks/Stop")
                    .header("authorization", format!("bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["ok"], true);
        assert_eq!(v["seq"], 1);
    }

    #[tokio::test]
    async fn unknown_route_returns_json_404_with_hint() {
        let (status, v) = get(router(test_state().await), "/no-such-endpoint").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(v["error"].as_str().unwrap().contains("no route"));
        assert!(v["hint"].as_str().unwrap().contains("/docs"));
    }

    #[tokio::test]
    async fn create_400s_name_the_field() {
        let app = router(test_state().await);
        let (s, v) = call(app.clone(), "POST", "/sessions", Some(r#"{"cwd": "relative"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("cwd"), "{v}");
        let (s, v) = call(app.clone(), "POST", "/sessions", Some(r#"{"cwd": "/tmp", "project": "x"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("project"), "{v}");
        let (s, v) = call(app.clone(), "POST", "/sessions", Some(r#"{"cwd": "/tmp", "permission_mode": "yolo"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("permission_mode"), "{v}");
        let (s, v) = call(app, "POST", "/sessions", Some("not json")).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("bad body"), "{v}");
    }

    // --- parity tests (ported from the WS dispatcher) ---

    #[tokio::test]
    async fn status_404_for_unknown() {
        let (status, _) = get(router(test_state().await), "/sessions/nope/status").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_idle_for_fresh_session() {
        let st = test_state().await;
        seed(&st, "s1").await;
        let (status, b) = get(router(st), "/sessions/s1/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(b["status"], "idle");
        assert_eq!(b["cursor"], 0);
        assert!(b["deciding_event"].is_null());
    }

    #[tokio::test]
    async fn wait_returns_when_ended() {
        let st = test_state().await;
        seed(&st, "s2").await;
        crate::events_store::mark_session_ended(&st.db, "s2", "2026-05-27T01:00:00Z").await.unwrap();
        let (status, b) = get(router(st), "/sessions/s2/wait?since=0&timeout=10").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(b["status"], "ended");
        assert!(b.get("timed_out").is_none());
    }

    #[tokio::test]
    async fn messages_poll_filters_transcript() {
        let st = test_state().await;
        seed(&st, "s3").await;
        for kind in ["user_prompt", "notification", "assistant_message", "permission_request"] {
            append(&st, "s3", kind).await;
        }
        let (status, b) = get(router(st), "/sessions/s3/messages?offset=0&size=500&wait=0").await;
        assert_eq!(status, StatusCode::OK);
        let kinds: Vec<&str> = b["messages"].as_array().unwrap().iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, vec!["user_prompt", "assistant_message"]);
    }

    fn event_seqs(b: &Value) -> Vec<i64> {
        b["events"].as_array().unwrap().iter().map(|e| e["seq"].as_i64().unwrap()).collect()
    }

    #[tokio::test]
    async fn events_400_on_missing_or_bad_paging() {
        let st = test_state().await;
        seed(&st, "e1").await;
        let app = router(st);
        for q in ["size=10", "offset=0", "offset=0&size=0", "offset=-2&size=10"] {
            let (status, b) = get(app.clone(), &format!("/sessions/e1/events?{q}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{q}");
            assert!(b["error"].as_str().unwrap().contains("offset"), "{q}: {b}");
        }
    }

    #[tokio::test]
    async fn events_size_capped_tail_and_cursor_pages() {
        let st = test_state().await;
        seed(&st, "e5").await;
        for _ in 0..6 {
            append(&st, "e5", "user_prompt").await;
        }
        let app = router(st);
        let (status, b) = get(app.clone(), "/sessions/e5/events?offset=0&size=9999&wait=0").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(b["events"].as_array().unwrap().len(), 6);
        let (_, b) = get(app.clone(), "/sessions/e5/events?offset=-1&size=3&wait=0").await;
        assert_eq!(event_seqs(&b), vec![4, 5, 6]);
        assert_eq!(b["next_cursor"], 6);
        let (_, b) = get(app.clone(), "/sessions/e5/events?offset=2&size=2&wait=0").await;
        assert_eq!(event_seqs(&b), vec![3, 4]);
        assert_eq!(b["next_cursor"], 4);
        // follow is ignored for explicit cursor reads
        let (_, b) = get(app, "/sessions/e5/events?offset=4&size=5&wait=0&follow=1").await;
        assert_eq!(event_seqs(&b), vec![5, 6]);
    }

    #[tokio::test]
    async fn events_tail_empty_session_returns_empty() {
        let st = test_state().await;
        seed(&st, "e8").await;
        let (status, b) = get(router(st), "/sessions/e8/events?offset=-1&size=5&wait=0").await;
        assert_eq!(status, StatusCode::OK);
        assert!(b["events"].as_array().unwrap().is_empty());
        assert_eq!(b["next_cursor"], 0);
    }

    /// `/status` and `/wait` must expose the BOUNDARY cursor, not just the tail.
    #[tokio::test]
    async fn wait_returns_boundary_cursor_distinct_from_tail() {
        let st = test_state().await;
        seed(&st, "b1").await;
        append(&st, "b1", "user_prompt").await; // 1
        let stop = append(&st, "b1", "stop").await; // 2 — the boundary
        let tail = append(&st, "b1", "assistant_message").await; // 3 — the tail
        assert_eq!((stop, tail), (2, 3));
        let app = router(st);
        let (_, s) = get(app.clone(), "/sessions/b1/status").await;
        assert_eq!(s["cursor"], 3);
        assert_eq!(s["boundary_cursor"], 2);
        let (_, w) = get(app.clone(), "/sessions/b1/wait?since=0&timeout=2").await;
        assert_eq!(w["status"], "idle");
        assert_eq!(w["boundary_cursor"], 2);
        let (_, w2) = get(app, "/sessions/b1/wait?since=2&timeout=1").await;
        assert_eq!(w2["timed_out"], true);
        assert_eq!(w2["boundary_cursor"], 2);
    }

    #[tokio::test]
    async fn stale_stop_does_not_satisfy_wait_armed_at_boundary() {
        let st = test_state().await;
        seed(&st, "b2").await;
        append(&st, "b2", "user_prompt").await; // 1
        append(&st, "b2", "stop").await; // 2 — previous turn
        let app = router(st.clone());
        let (_, s) = get(app.clone(), "/sessions/b2/status").await;
        let since = s["boundary_cursor"].as_i64().unwrap();
        assert_eq!(since, 2);
        let (_, w) = get(app.clone(), &format!("/sessions/b2/wait?since={since}&timeout=1")).await;
        assert_eq!(w["timed_out"], true);
        append(&st, "b2", "user_prompt").await; // 3
        append(&st, "b2", "stop").await; // 4
        let (_, w2) = get(app, &format!("/sessions/b2/wait?since={since}&timeout=2")).await;
        assert!(w2.get("timed_out").is_none());
        assert_eq!(w2["boundary_cursor"], 4);
    }

    #[tokio::test]
    async fn tail_follow_long_polls_for_new_events_only() {
        let st = test_state().await;
        seed(&st, "b3").await;
        append(&st, "b3", "user_prompt").await; // 1 — pre-existing
        let st2 = st.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            append(&st2, "b3", "stop").await; // 2 — arrives during the poll
            st2.bus.notify("b3").await;
        });
        let (_, b) = get(router(st), "/sessions/b3/events?offset=-1&size=5&wait=5&follow=1").await;
        let kinds: Vec<&str> = b["events"].as_array().unwrap().iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, vec!["stop"]);
        assert_eq!(b["next_cursor"], 2);
    }

    #[tokio::test]
    async fn tail_without_follow_is_unchanged() {
        let st = test_state().await;
        seed(&st, "b4").await;
        append(&st, "b4", "user_prompt").await;
        let (_, b) = get(router(st), "/sessions/b4/events?offset=-1&size=5&wait=1").await;
        assert_eq!(b["events"].as_array().unwrap().len(), 1);
        assert_eq!(b["next_cursor"], 1);
    }

    /// Lost-wakeup coverage: an event committed *after* the page call has begun
    /// must wake it.
    #[tokio::test]
    async fn page_wakes_for_event_committed_after_call_starts() {
        let st = test_state().await;
        seed(&st, "b5").await;
        let st2 = st.clone();
        let writer = tokio::spawn(async move {
            append(&st2, "b5", "stop").await;
            st2.bus.notify("b5").await;
        });
        let (_, b) = get(router(st), "/sessions/b5/events?offset=0&size=5&wait=10").await;
        writer.await.unwrap();
        let kinds: Vec<&str> = b["events"].as_array().unwrap().iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, vec!["stop"]);
    }

    #[tokio::test]
    async fn sse_stream_emits_first_frame_and_404s_unknown() {
        let st = test_state().await;
        seed(&st, "sse1").await;
        append(&st, "sse1", "user_prompt").await;
        let app = router(st);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sessions/sse1/events/stream?since=0")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("text/event-stream"));
        let mut body = resp.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(2), http_body_util::BodyExt::frame(&mut body))
            .await
            .expect("first frame in time")
            .unwrap()
            .unwrap();
        let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(text.starts_with("event: user_prompt"), "{text}");
        assert!(text.contains("id: 1"));

        let (status, _) = get(app, "/sessions/nope/events/stream?since=0").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn permission_resolve_validates_behavior() {
        let st = test_state().await;
        let app = router(st);
        let (s, v) = call(app.clone(), "POST", "/sessions/x/permission_requests/req-1", Some(r#"{"behavior":"maybe"}"#)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("behavior"));
        let (s, _) = call(app, "POST", "/sessions/x/permission_requests/req-1", Some(r#"{"behavior":"allow"}"#)).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn health_reports_version_and_session_count() {
        let (s, v) = get(router(test_state().await), "/health").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v["ok"], true);
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["sessions"], 0);
        assert_eq!(v, json!({"ok": true, "version": env!("CARGO_PKG_VERSION"), "sessions": 0, "hosts": 0, "watches": 0}));
    }
}
