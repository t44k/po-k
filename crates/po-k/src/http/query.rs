//! Query-string parsing shared by the page and wait endpoints. Kept
//! hand-rolled so the exact 400 messages stay stable.

use crate::core::{self, CoreError};

/// Required `offset` + `size` (and optional `wait`, `follow`). `offset >= -1`
/// (`-1` = tail), `size > 0`; anything else is a 400. `wait` defaults to
/// [`core::events::DEFAULT_WAIT`]. `follow=1` pins a tail request to the
/// session's current cursor so it long-polls for *new* events.
pub fn page_params(query: &str) -> Result<(i64, i64, u64, bool), CoreError> {
    let offset = qget(query, "offset").and_then(|s| s.parse::<i64>().ok());
    let size = qget(query, "size").and_then(|s| s.parse::<i64>().ok());
    let wait = qget(query, "wait")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(core::events::DEFAULT_WAIT);
    let follow = matches!(qget(query, "follow").as_deref(), Some("1") | Some("true") | Some("yes"));
    match (offset, size) {
        (Some(o), Some(s)) if o >= -1 && s > 0 => Ok((o, s, wait, follow)),
        (Some(_), Some(_)) => Err(CoreError::BadRequest("offset must be >= -1 and size must be > 0".into())),
        _ => Err(CoreError::BadRequest("offset and size query parameters are required".into())),
    }
}

/// `since` (default 0) and `timeout` (default from core) for `/wait`.
pub fn wait_params(query: &str) -> (i64, u64) {
    let since = qget(query, "since").and_then(|s| s.parse().ok()).unwrap_or(0);
    let timeout = core::control::wait_defaults(qget(query, "timeout").and_then(|s| s.parse().ok()));
    (since, timeout)
}

pub fn since_param(query: &str) -> i64 {
    qget(query, "since").and_then(|s| s.parse().ok()).unwrap_or(0)
}

pub fn qget(query: &str, key: &str) -> Option<String> {
    serde_urlencoded::from_str::<Vec<(String, String)>>(query)
        .ok()?
        .into_iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing() {
        assert_eq!(qget("offset=5&wait=0", "offset"), Some("5".into()));
        assert_eq!(page_params("offset=7&size=100&wait=10").unwrap(), (7, 100, 10, false));
        assert_eq!(page_params("offset=-1&size=50").unwrap(), (-1, 50, core::events::DEFAULT_WAIT, false));
        assert_eq!(wait_params("since=4&timeout=9"), (4, 9));
        assert_eq!(wait_params(""), (0, 60));
    }

    #[test]
    fn follow_param_parsing() {
        for q in ["offset=-1&size=5", "offset=-1&size=5&follow=0", "offset=-1&size=5&follow=off"] {
            assert!(!page_params(q).unwrap().3, "{q}");
        }
        for q in ["offset=-1&size=5&follow=1", "offset=-1&size=5&follow=true", "offset=-1&size=5&follow=yes"] {
            assert!(page_params(q).unwrap().3, "{q}");
        }
    }

    #[test]
    fn page_params_rejects_missing_and_out_of_range() {
        for q in ["size=10", "offset=0", "", "wait=5"] {
            let e = page_params(q).unwrap_err();
            assert_eq!(e.status(), 400);
            assert!(e.to_string().contains("required"), "{q:?}: {e}");
        }
        for q in ["offset=-2&size=10", "offset=0&size=0", "offset=0&size=-5"] {
            let e = page_params(q).unwrap_err();
            assert_eq!(e.status(), 400);
            assert!(e.to_string().contains(">="), "{q:?}: {e}");
        }
    }
}
