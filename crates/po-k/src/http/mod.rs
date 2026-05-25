//! HTTP router assembly.
//!
//! Two layers:
//!   - `/health` is unauthenticated.
//!   - everything else is registered on the protected router (bearer auth).
//!
//! Phases hang their endpoints off the protected sub-router. Currently:
//! `/health` (public) + `/projects` (protected, config-driven).

use axum::middleware;
use axum::routing::{delete, get, post};
use axum::Router;

use crate::auth::require_bearer;
use crate::state::AppState;

pub mod events;
pub mod health;
pub mod hooks_in;
pub mod messages;
pub mod perms;
pub mod projects;
pub mod sessions;

pub fn router(state: AppState) -> Router {
    let public = Router::new().route("/health", get(health::handler));

    let protected = Router::new()
        .route("/projects", get(projects::list))
        .route("/sessions", post(sessions::create).get(sessions::list))
        .route("/sessions/{id}", get(sessions::detail).delete(sessions::delete))
        .route("/sessions/{id}/messages", post(messages::message))
        .route("/sessions/{id}/interrupt", post(messages::interrupt))
        .route("/sessions/{id}/clear", post(messages::clear))
        .route("/sessions/{id}/files", post(messages::upload_file))
        .route("/sessions/{id}/hooks/{event}", post(hooks_in::ingest))
        .route("/sessions/{id}/events", get(events::poll))
        .route("/sessions/{id}/events/stream", get(events::stream))
        .route("/sessions/{id}/cost", get(events::cost))
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

    public.merge(protected)
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
