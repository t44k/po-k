//! HTTP router assembly.
//!
//! Two layers:
//!   - `/health` is unauthenticated.
//!   - everything else is registered on the protected router (bearer auth).
//!
//! Phases hang their endpoints off the protected sub-router. Currently:
//! `/health` (public) + `/projects` (protected, config-driven).

use axum::http::{Method, StatusCode, Uri};
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::auth::require_bearer;
use crate::core::{CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

/// Adapt a core result into an Axum `(StatusCode, Json)` response, preserving
/// the core-assigned status code (200/201/4xx/5xx) and JSON body verbatim.
pub(crate) fn adapt(r: CoreResult<CoreResponse>) -> (StatusCode, Json<Value>) {
    match r {
        Ok(ok) => (
            StatusCode::from_u16(ok.status).unwrap_or(StatusCode::OK),
            Json(ok.body),
        ),
        Err(e) => adapt_err(e),
    }
}

pub(crate) fn adapt_err(e: CoreError) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(e.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(e.body()),
    )
}

pub mod control;
pub mod events;
pub mod health;
pub mod help;
pub mod hooks_in;
pub mod messages;
pub mod perms;
pub mod projects;
pub mod sessions;

pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route("/health", get(health::handler))
        .route("/help", get(help::handler));

    let protected = Router::new()
        .route("/projects", get(projects::list))
        .route("/sessions", post(sessions::create).get(sessions::list))
        .route("/sessions/{id}", get(sessions::detail).delete(sessions::delete))
        .route(
            "/sessions/{id}/messages",
            post(messages::message).get(events::messages_poll),
        )
        .route("/sessions/{id}/messages/stream", get(events::messages_stream))
        .route("/sessions/{id}/interrupt", post(messages::interrupt))
        .route("/sessions/{id}/clear", post(messages::clear))
        .route("/sessions/{id}/files", post(messages::upload_file))
        .route("/sessions/{id}/hooks/{event}", post(hooks_in::ingest))
        .route("/sessions/{id}/events", get(events::poll))
        .route("/sessions/{id}/events/stream", get(events::stream))
        .route("/sessions/{id}/cost", get(events::cost))
        .route("/sessions/{id}/status", get(control::status))
        .route("/sessions/{id}/wait", get(control::wait))
        .route("/sessions/{id}/pane", get(control::pane))
        .route("/sessions/{id}/capabilities", get(sessions::capabilities))
        .route("/sessions/{id}/mcp/approve", post(perms::approve))
        .route(
            "/sessions/{id}/permission_requests/{req_id}",
            post(perms::resolve),
        )
        .route_layer(middleware::from_fn_with_state(
            state.token.clone(),
            require_bearer,
        ))
        .with_state(state);

    public.merge(protected).fallback(not_found)
}

/// Fallback for unmatched routes — axum's default returns an empty 404, which
/// is indistinguishable on the client from "endpoint returned empty data".
/// This handler returns a JSON body pointing at /help so callers immediately
/// see *why* they got nothing.
async fn not_found(method: Method, uri: Uri) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("no route for {method} {}", uri.path()),
            "hint": "GET /help lists every endpoint",
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Token;
    use crate::config::{Config, Project};
    use axum::body::Body;
    use axum::http::{header, Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::path::PathBuf;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let token = Token::__test_new("test-secret".into());
        let cfg = Config {
            projects: vec![Project {
                name: "po-k".into(),
                cwd: "/workspace".into(),
                model: None,
                effort: None,
                add_dirs: vec![],
                zellij_session: None,
            }],
            ..Default::default()
        };
        let db_path =
            std::env::temp_dir().join(format!("po-k-http-test-{}.db", uuid::Uuid::new_v4()));
        let db = crate::events_store::open(&db_path).await.unwrap();
        AppState::new(token, cfg, PathBuf::from("/dev/null"), db)
    }

    async fn body_json(resp: axum::http::Response<Body>) -> Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        (status, body_json(resp).await)
    }

    async fn seed_session(state: &AppState, sid: &str) {
        crate::events_store::insert_session(
            &state.db,
            &crate::events_store::SessionRow {
                sid: sid.into(),
                project: "po-k".into(),
                cwd: "/workspace".into(),
                zellij_session: "po-k-po-k".into(),
                model: None,
                effort: None,
                started_at: "2026-05-27T00:00:00Z".into(),
                ended_at: None,
                pid: None,
                last_event_seq: 0,
                profiles: None,
                plugin_dir: None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn status_404_for_unknown_session() {
        let app = router(test_state().await);
        let (status, _) = get_json(app, "/sessions/nope/status").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_idle_for_fresh_session() {
        let state = test_state().await;
        seed_session(&state, "s1").await;
        let (status, body) = get_json(router(state), "/sessions/s1/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "idle");
        assert_eq!(body["cursor"], 0);
        assert!(body["deciding_event"].is_null());
    }

    #[tokio::test]
    async fn wait_returns_immediately_when_ended() {
        let state = test_state().await;
        seed_session(&state, "s2").await;
        crate::events_store::mark_session_ended(&state.db, "s2", "2026-05-27T01:00:00Z")
            .await
            .unwrap();
        let (status, body) =
            get_json(router(state), "/sessions/s2/wait?since=0&timeout=10").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ended");
        assert!(body.get("timed_out").is_none());
    }

    #[tokio::test]
    async fn wait_ignores_stale_stop() {
        // A stop from a PRIOR turn (seq 1) must not satisfy wait(since=1): the
        // race guard requires the deciding boundary to be newer than `since`.
        let state = test_state().await;
        seed_session(&state, "s3").await;
        crate::events_store::append_event(&state.db, "s3", "t", "stop", &serde_json::json!({}))
            .await
            .unwrap();
        let (status, body) =
            get_json(router(state), "/sessions/s3/wait?since=1&timeout=1").await;
        assert_eq!(status, StatusCode::OK);
        // Did not satisfy → timed out with the (stale) idle status.
        assert_eq!(body["timed_out"], true);
        assert_eq!(body["status"], "idle");
    }

    #[tokio::test]
    async fn messages_poll_filters_to_transcript() {
        let state = test_state().await;
        seed_session(&state, "s4").await;
        for (kind, _) in [
            ("user_prompt", 1),
            ("notification", 2),
            ("assistant_message", 3),
            ("permission_request", 4),
        ] {
            crate::events_store::append_event(&state.db, "s4", "t", kind, &serde_json::json!({}))
                .await
                .unwrap();
        }
        let (status, body) =
            get_json(router(state), "/sessions/s4/messages?since=0&wait=0").await;
        assert_eq!(status, StatusCode::OK);
        let kinds: Vec<&str> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, vec!["user_prompt", "assistant_message"]);
    }

    #[tokio::test]
    async fn health_is_public() {
        let app = router(test_state().await);
        let resp = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_requires_bearer() {
        let app = router(test_state().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        // Body should be JSON with a useful error message — empty bodies make
        // 401 look identical to "endpoint returned nothing" on the client.
        let v = body_json(resp).await;
        assert!(
            v["error"].as_str().unwrap_or("").contains("Authorization"),
            "401 body lacks helpful error: {v}"
        );
    }

    #[tokio::test]
    async fn invalid_bearer_returns_json_401() {
        let app = router(test_state().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/projects")
                    .header(header::AUTHORIZATION, "Bearer not-the-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let v = body_json(resp).await;
        assert_eq!(v["error"], "invalid bearer token");
    }

    #[tokio::test]
    async fn unknown_route_returns_json_404() {
        let app = router(test_state().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/no-such-endpoint")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let v = body_json(resp).await;
        assert!(v["error"].as_str().unwrap().contains("no route"));
        assert!(v["hint"].as_str().unwrap().contains("/help"));
    }

    #[tokio::test]
    async fn projects_returns_configured_list() {
        let app = router(test_state().await);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/projects")
                    .header(header::AUTHORIZATION, "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "po-k");
        assert_eq!(arr[0]["cwd"], "/workspace");
        assert!(arr[0]["session_ids"].is_array());
    }
}
