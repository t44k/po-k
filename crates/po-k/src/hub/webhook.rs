//! Signed webhook delivery to the orchestrator (Hermes' generic webhook
//! adapter).
//!
//! * **Metadata only.** The envelope carries ids, status and the deciding
//!   event — never CC prose. The woken turn fetches session content itself.
//! * **The signature covers the exact bytes sent.** Serialise once, HMAC it,
//!   send that buffer.
//! * **One request id per attempt.** `x-request-id` is `<notification_id>:<attempt>`:
//!   a transport retry of the same attempt collapses on the receiver, while a
//!   deliberate replay (unacknowledged for `ack_timeout`) gets a new id so the
//!   receiver's duplicate cache does not swallow it.
//! * **Secrets are referenced, never stored.** Only an env var name or file
//!   path lives in the database; the value is read at send time.

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;

use super::store::WebhookTarget;

pub const EVENT_TYPE: &str = "pok_notification";
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Transport/5xx backoff: 30s, 60s, 2m, 4m, 8m, then a 15m ceiling.
pub fn backoff_secs(attempt: i64) -> i64 {
    match attempt {
        0 | 1 => 30,
        2 => 60,
        3 => 120,
        4 => 240,
        5 => 480,
        _ => 900,
    }
}

/// Hex HMAC-SHA256 of `body` under `secret` — the `x-webhook-signature` value.
pub fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Delivered,
    /// The receiver already had this request id and started no new turn.
    Duplicate,
    Retry(String),
    /// Config problem (bad route, bad signature, bad request): retrying cannot help.
    Fatal(String),
}

pub fn classify(status: u16, body: &str) -> Outcome {
    if (200..300).contains(&status) {
        let duplicate = serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "duplicate"))
            .unwrap_or(false);
        return if duplicate { Outcome::Duplicate } else { Outcome::Delivered };
    }
    let snippet: String = body.chars().take(160).collect();
    match status {
        400 | 401 | 403 | 404 | 405 | 410 | 422 => Outcome::Fatal(format!("HTTP {status}: {snippet}")),
        _ => Outcome::Retry(format!("HTTP {status}: {snippet}")),
    }
}

/// Read the HMAC secret. Env var wins over file. Never logged.
pub fn resolve_secret(target: &WebhookTarget) -> Result<String, String> {
    if let Some(name) = target.secret_env.as_deref().filter(|n| !n.is_empty()) {
        return match std::env::var(name) {
            Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
            _ => Err(format!("secret env var {name:?} is unset or empty in the po-k serve environment")),
        };
    }
    if let Some(path) = target.secret_file.as_deref().filter(|p| !p.is_empty()) {
        return match std::fs::read_to_string(path) {
            Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
            Ok(_) => Err(format!("secret file {path:?} is empty")),
            Err(e) => Err(format!("secret file {path:?} unreadable: {e}")),
        };
    }
    Err("webhook has neither secret_env nor secret_file".into())
}

/// Validate a target as supplied by the API (URL scheme + a secret reference).
pub fn validate_target(t: &WebhookTarget) -> Result<(), String> {
    if !(t.url.starts_with("http://") || t.url.starts_with("https://")) {
        return Err(format!("webhook.url must be http(s), got {:?}", t.url));
    }
    let has_env = t.secret_env.as_deref().is_some_and(|s| !s.is_empty());
    let has_file = t.secret_file.as_deref().is_some_and(|s| !s.is_empty());
    if !has_env && !has_file {
        return Err("webhook needs secret_env or secret_file (unsigned webhooks are refused)".into());
    }
    Ok(())
}

/// One POST. Never touches watch state.
pub async fn post_once(client: &reqwest::Client, target: &WebhookTarget, request_id: &str, body: &[u8], secret: &str) -> Outcome {
    let res = client
        .post(&target.url)
        .header("content-type", "application/json")
        .header("x-webhook-signature", sign_body(secret, body))
        .header("x-request-id", request_id)
        .header("x-pok-event", EVENT_TYPE)
        .timeout(REQUEST_TIMEOUT)
        .body(body.to_vec())
        .send()
        .await;
    match res {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            classify(status, &text)
        }
        Err(e) if e.is_timeout() => Outcome::Retry(format!("timeout after {REQUEST_TIMEOUT:?}")),
        Err(e) => Outcome::Retry(format!("request failed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_body_matches_known_hmac_vector() {
        assert_eq!(
            sign_body("key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn backoff_is_monotonic_and_capped() {
        let seq: Vec<i64> = (1..=8).map(backoff_secs).collect();
        assert_eq!(seq, vec![30, 60, 120, 240, 480, 900, 900, 900]);
    }

    #[test]
    fn classify_maps_responses() {
        assert_eq!(classify(200, r#"{"status":"accepted"}"#), Outcome::Delivered);
        assert_eq!(classify(202, ""), Outcome::Delivered);
        assert_eq!(classify(200, r#"{"status":"duplicate"}"#), Outcome::Duplicate);
        for s in [400u16, 401, 403, 404, 405, 410, 422] {
            assert!(matches!(classify(s, "nope"), Outcome::Fatal(_)), "HTTP {s}");
        }
        for s in [408u16, 429, 500, 502, 503, 504] {
            assert!(matches!(classify(s, "boom"), Outcome::Retry(_)), "HTTP {s}");
        }
    }

    #[test]
    fn resolve_secret_prefers_env_then_file() {
        let mut t = WebhookTarget { url: "http://x".into(), secret_env: Some("POK_TEST_SECRET_A".into()), secret_file: None };
        std::env::set_var("POK_TEST_SECRET_A", "  from-env  ");
        assert_eq!(resolve_secret(&t).unwrap(), "from-env");
        std::env::remove_var("POK_TEST_SECRET_A");
        let err = resolve_secret(&t).unwrap_err();
        assert!(err.contains("POK_TEST_SECRET_A") && !err.contains("from-env"));
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("secret");
        std::fs::write(&p, "from-file\n").unwrap();
        t.secret_env = None;
        t.secret_file = Some(p.to_string_lossy().into());
        assert_eq!(resolve_secret(&t).unwrap(), "from-file");
        t.secret_file = None;
        assert!(resolve_secret(&t).is_err());
    }

    #[test]
    fn validate_target_requires_https_and_a_secret_ref() {
        let ok = WebhookTarget { url: "http://127.0.0.1:8644/w".into(), secret_env: Some("S".into()), secret_file: None };
        assert!(validate_target(&ok).is_ok());
        let no_secret = WebhookTarget { url: "http://x".into(), ..Default::default() };
        assert!(validate_target(&no_secret).is_err());
        let bad_scheme = WebhookTarget { url: "ftp://x".into(), secret_env: Some("S".into()), secret_file: None };
        assert!(validate_target(&bad_scheme).is_err());
    }
}
