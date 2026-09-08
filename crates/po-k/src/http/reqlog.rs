//! Request log: one `INFO` line per HTTP call with method, path, query,
//! caller address, the calling po-k's version (when it is a po-k), status,
//! duration and the JSON body — redacted and truncated — so an operator can
//! see exactly which parameters a call carried. `POK_LOG_REQUESTS=0` turns it
//! off; the `Authorization` header is never logged.

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{ConnectInfo, Request};
use axum::http::{header, Method};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Instant;

/// Bodies above this are passed through without being read for the log.
const BUFFER_LIMIT: usize = 64 * 1024;
/// Logged body length (after redaction), in characters.
const LOG_BODY_MAX: usize = 2048;
/// CC hook payloads arrive on every tool call; keep them short.
const LOG_HOOK_BODY_MAX: usize = 300;
/// A JSON key containing any of these (case-insensitive) is logged as `***`.
const SECRET_KEY_PARTS: &[&str] = &["token", "secret", "authorization", "password", "api_key", "apikey", "bearer", "credential"];

pub fn enabled() -> bool {
    match std::env::var("POK_LOG_REQUESTS") {
        Ok(v) => !matches!(v.trim(), "0" | "false" | "off" | "no"),
        Err(_) => true,
    }
}

pub async fn log(req: Request, next: Next) -> Response {
    if !enabled() {
        return next.run(req).await;
    }
    let started = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let from = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.to_string())
        .unwrap_or_else(|| "-".to_string());
    let pok_version = req
        .headers()
        .get(crate::version::HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let authed = req.headers().contains_key(header::AUTHORIZATION);

    let (req, body_summary) = capture_body(req, &path).await;
    let resp = next.run(req).await;
    let ms = started.elapsed().as_millis();
    tracing::info!(
        target: "po_k::http",
        method = %method,
        path = %path,
        query = %query,
        from = %from,
        pok = %pok_version,
        auth = authed,
        status = resp.status().as_u16(),
        ms,
        body = %body_summary,
        "request"
    );
    resp
}

/// Read a small JSON body for the log and put it back untouched.
async fn capture_body(req: Request, path: &str) -> (Request, String) {
    if !matches!(*req.method(), Method::POST | Method::PUT | Method::PATCH) {
        return (req, String::new());
    }
    let declared: Option<usize> = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    match declared {
        Some(0) => return (req, String::new()),
        Some(n) if n > BUFFER_LIMIT => return (req, format!("<{n} bytes, not logged>")),
        _ => {}
    }
    let (parts, body) = req.into_parts();
    let bytes: Bytes = match to_bytes(body, BUFFER_LIMIT).await {
        Ok(b) => b,
        Err(e) => {
            // Body unreadable (too long without content-length, or the client
            // went away): hand the handler an empty body; it will answer 4xx.
            return (Request::from_parts(parts, Body::empty()), format!("<unreadable: {e}>"));
        }
    };
    let max = if path.contains("/hooks/") { LOG_HOOK_BODY_MAX } else { LOG_BODY_MAX };
    let summary = summarize(&bytes, max);
    (Request::from_parts(parts, Body::from(bytes)), summary)
}

/// Redacted, compact, truncated rendering of a request body.
pub fn summarize(bytes: &[u8], max: usize) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let rendered = match serde_json::from_slice::<Value>(bytes) {
        Ok(mut v) => {
            redact(&mut v);
            v.to_string()
        }
        Err(_) => format!("<{} bytes, not JSON>", bytes.len()),
    };
    truncate(rendered, max)
}

fn truncate(s: String, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s;
    }
    let head: String = s.chars().take(max).collect();
    format!("{head}…(+{} chars)", n - max)
}

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    SECRET_KEY_PARTS.iter().any(|p| k.contains(p))
}

/// Replace the value of every secret-looking key, at any depth, with `***`.
pub fn redact(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if is_secret_key(k) {
                    *val = Value::String("***".into());
                } else {
                    redact(val);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_secret_keys_at_any_depth_and_keeps_the_rest() {
        let mut v = json!({
            "cwd": "/w",
            "token": "abc",
            "mcp_servers": { "linear": { "env": { "LINEAR_ACCESS_TOKEN": "x", "MODE": "ro" }, "headers": { "Authorization": "Bearer y" } } },
            "webhook": { "url": "http://h", "secret_env": "POK_WEBHOOK_SECRET" },
            "list": [{ "api_key": "k", "name": "n" }]
        });
        redact(&mut v);
        assert_eq!(v["cwd"], "/w");
        assert_eq!(v["token"], "***");
        assert_eq!(v["mcp_servers"]["linear"]["env"]["LINEAR_ACCESS_TOKEN"], "***");
        assert_eq!(v["mcp_servers"]["linear"]["env"]["MODE"], "ro");
        assert_eq!(v["mcp_servers"]["linear"]["headers"]["Authorization"], "***");
        assert_eq!(v["webhook"]["url"], "http://h");
        // The *name* of the env var is a secret-looking key: redacted too. Fine.
        assert_eq!(v["webhook"]["secret_env"], "***");
        assert_eq!(v["list"][0]["api_key"], "***");
        assert_eq!(v["list"][0]["name"], "n");
    }

    #[test]
    fn summarize_truncates_and_marks_non_json() {
        let long = json!({ "text": "x".repeat(5000) }).to_string();
        let s = summarize(long.as_bytes(), 100);
        assert!(s.chars().count() < 130, "{s}");
        assert!(s.contains("…(+"), "{s}");
        assert_eq!(summarize(b"not json", 100), "<8 bytes, not JSON>");
        assert_eq!(summarize(b"", 100), "");
    }
}
