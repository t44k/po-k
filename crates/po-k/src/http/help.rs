//! `GET /help` — the API reference, served from a single Markdown source.
//!
//! Defaults to `text/plain; charset=utf-8` (friendly to `curl`); returns a
//! JSON wrapper `{format, version, content}` when the client sets
//! `Accept: application/json`. One source of truth — `help.md` — so the two
//! formats can't drift.

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

const HELP_MD: &str = include_str!("help.md");

pub async fn handler(headers: HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/json"))
        .unwrap_or(false);

    if wants_json {
        Json(json!({
            "format": "markdown",
            "version": env!("CARGO_PKG_VERSION"),
            "content": HELP_MD,
        }))
        .into_response()
    } else {
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            HELP_MD,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new().route("/help", get(handler))
    }

    #[tokio::test]
    async fn default_returns_markdown_as_text_plain() {
        let resp = app()
            .oneshot(Request::builder().uri("/help").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(ct.starts_with("text/plain"), "content-type was {ct:?}");
        let body = String::from_utf8(
            resp.into_body().collect().await.unwrap().to_bytes().to_vec(),
        )
        .unwrap();
        assert!(body.contains("# po-k HTTP API"));
        // Spot-check a few documented endpoints so a forgotten doc update
        // for a new endpoint is at least likely to be noticed.
        for needle in [
            "GET /health",
            "GET /sessions/{id}/status",
            "GET /sessions/{id}/wait",
            "GET /sessions/{id}/messages",
            "POST /sessions/{id}/messages",
            "GET /sessions/{id}/pane",
            "Restart behaviour",
        ] {
            assert!(body.contains(needle), "help.md missing {needle:?}");
        }
    }

    #[tokio::test]
    async fn accept_json_returns_wrapped_markdown() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/help")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["format"], "markdown");
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert!(v["content"].as_str().unwrap().contains("# po-k HTTP API"));
    }
}
