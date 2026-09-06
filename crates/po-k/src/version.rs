//! Version handshake between po-k processes.
//!
//! Every po-k → po-k HTTP request (hub → remote box, `po-k mcp` → local
//! serve) carries `x-pok-version: <this build>`. The receiving server rejects a
//! different version with `409 {"error": "version mismatch …"}` before doing
//! anything else, and answers every response with its own version header. The
//! hub additionally compares the remote `/health` version when connecting a
//! host. Mixed builds therefore fail loudly at connection time instead of
//! misbehaving later.

use axum::body::Body;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const HEADER: &str = "x-pok-version";

/// `Err(message)` when `remote` (from a header or `/health`) differs from us.
pub fn check(remote: &str, remote_label: &str) -> Result<(), String> {
    let remote = remote.trim();
    if remote == VERSION {
        Ok(())
    } else {
        Err(format!(
            "version mismatch: this po-k is {VERSION}, {remote_label} runs {} — deploy the same build to both",
            if remote.is_empty() { "an unknown version (pre-0.12)" } else { remote }
        ))
    }
}

/// axum middleware: refuse a mismatching client, stamp our version on replies.
pub async fn enforce(req: Request<Body>, next: Next) -> Response {
    if let Some(v) = req.headers().get(HEADER).and_then(|v| v.to_str().ok()) {
        if let Err(msg) = check(v, "the calling po-k") {
            let mut resp = (
                StatusCode::CONFLICT,
                Json(json!({ "error": msg, "server_version": VERSION, "client_version": v.trim() })),
            )
                .into_response();
            resp.headers_mut().insert(HEADER, HeaderValue::from_static(VERSION));
            return resp;
        }
    }
    let mut resp = next.run(req).await;
    resp.headers_mut().insert(HEADER, HeaderValue::from_static(VERSION));
    resp
}

/// Attach the version header to an outgoing po-k → po-k request.
pub fn tag(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    req.header(HEADER, VERSION)
}

/// Is this error body a version-mismatch rejection from another po-k?
pub fn is_mismatch_body(status: u16, body: &str) -> bool {
    status == 409 && body.contains("version mismatch")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_accepts_same_and_names_both_versions_otherwise() {
        assert!(check(VERSION, "x").is_ok());
        assert!(check(&format!(" {VERSION} "), "x").is_ok());
        let e = check("0.11.0", "host box.zrz").unwrap_err();
        assert!(e.contains(VERSION) && e.contains("0.11.0") && e.contains("box.zrz"), "{e}");
        assert!(check("", "host").unwrap_err().contains("pre-0.12"));
        assert!(is_mismatch_body(409, r#"{"error":"version mismatch: …"}"#));
        assert!(!is_mismatch_body(409, r#"{"error":"a session named x is already running"}"#));
    }
}
