//! WebSocket request dispatcher — the WS-side equivalent of the Axum router.
//! Maps an incoming `ws_request` `(method, path)` to a `core::` function and
//! returns either a unary response or an SSE-framed stream (for `/…/stream`).

use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::pin::Pin;
use std::sync::OnceLock;

use crate::core::{self, CoreError, CoreResponse, CoreResult};
use crate::state::AppState;

/// Result of dispatching a `ws_request`.
pub enum Dispatched {
    /// One `ws_response`.
    Unary { status: u16, body: String },
    /// A sequence of SSE frames → `ws_stream_chunk`s, then `ws_stream_end`.
    Stream(Pin<Box<dyn Stream<Item = String> + Send>>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    Projects,
    Sessions,
    Session,
    Messages,
    MessagesStream,
    Interrupt,
    Clear,
    Files,
    Events,
    EventsStream,
    Cost,
    Status,
    Wait,
    Pane,
    PermissionResolve,
    Capabilities,
}

fn routes() -> &'static matchit::Router<Route> {
    static R: OnceLock<matchit::Router<Route>> = OnceLock::new();
    R.get_or_init(|| {
        let mut r = matchit::Router::new();
        let mut add = |p: &str, route: Route| {
            r.insert(p.to_string(), route).expect("static route");
        };
        add("/projects", Route::Projects);
        add("/sessions", Route::Sessions);
        add("/sessions/{id}", Route::Session);
        add("/sessions/{id}/messages", Route::Messages);
        add("/sessions/{id}/messages/stream", Route::MessagesStream);
        add("/sessions/{id}/interrupt", Route::Interrupt);
        add("/sessions/{id}/clear", Route::Clear);
        add("/sessions/{id}/files", Route::Files);
        add("/sessions/{id}/events", Route::Events);
        add("/sessions/{id}/events/stream", Route::EventsStream);
        add("/sessions/{id}/cost", Route::Cost);
        add("/sessions/{id}/status", Route::Status);
        add("/sessions/{id}/wait", Route::Wait);
        add("/sessions/{id}/pane", Route::Pane);
        add(
            "/sessions/{id}/permission_requests/{req_id}",
            Route::PermissionResolve,
        );
        add("/sessions/{id}/capabilities", Route::Capabilities);
        r
    })
}

fn parse_body(body: Option<&str>) -> Value {
    body.and_then(|b| serde_json::from_str(b).ok())
        .unwrap_or(Value::Null)
}

fn unary(r: CoreResult<CoreResponse>) -> Dispatched {
    match r {
        Ok(ok) => Dispatched::Unary {
            status: ok.status,
            body: serde_json::to_string(&ok.body).unwrap_or_else(|_| "{}".into()),
        },
        Err(e) => Dispatched::Unary {
            status: e.status(),
            body: serde_json::to_string(&e.body()).unwrap_or_else(|_| "{}".into()),
        },
    }
}

fn not_found(method: &str, path: &str) -> Dispatched {
    Dispatched::Unary {
        status: 404,
        body: json!({ "error": format!("no route for {method} {path}") }).to_string(),
    }
}

/// Dispatch one request. `path` may include a query string.
pub async fn dispatch(
    state: &AppState,
    method: &str,
    full_path: &str,
    body: Option<&str>,
) -> Dispatched {
    let (path, query) = match full_path.split_once('?') {
        Some((p, q)) => (p, q),
        None => (full_path, ""),
    };
    let matched = match routes().at(path) {
        Ok(m) => m,
        Err(_) => return not_found(method, path),
    };
    let route = *matched.value;
    let id = matched.params.get("id").unwrap_or("").to_string();
    let req_id = matched.params.get("req_id").unwrap_or("").to_string();

    match (method, route) {
        ("GET", Route::Projects) => unary(core::projects::list(state).await),
        ("POST", Route::Sessions) => {
            let req = match serde_json::from_str(body.unwrap_or("{}")) {
                Ok(r) => r,
                Err(e) => {
                    return Dispatched::Unary {
                        status: 400,
                        body: json!({ "error": format!("bad body: {e}") }).to_string(),
                    }
                }
            };
            unary(core::sessions::create(state, req).await)
        }
        ("GET", Route::Sessions) => unary(core::sessions::list(state).await),
        ("GET", Route::Session) => unary(core::sessions::get(state, &id).await),
        ("DELETE", Route::Session) => unary(core::sessions::delete(state, &id).await),
        ("POST", Route::Messages) => {
            let text = parse_body(body)
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            unary(core::messages::send(state, &id, &text).await)
        }
        ("GET", Route::Messages) => {
            let (since, wait) = poll_params(query);
            unary(core::events::page(state, &id, true, since, wait).await)
        }
        ("POST", Route::Interrupt) => unary(core::messages::interrupt(state, &id).await),
        ("POST", Route::Clear) => unary(core::messages::clear(state, &id).await),
        ("POST", Route::Files) => {
            let b = parse_body(body);
            let filename = b.get("filename").and_then(|v| v.as_str()).unwrap_or("");
            let content = b.get("content_base64").and_then(|v| v.as_str()).unwrap_or("");
            unary(core::messages::upload_file(state, &id, filename, content).await)
        }
        ("GET", Route::Events) => {
            let (since, wait) = poll_params(query);
            unary(core::events::page(state, &id, false, since, wait).await)
        }
        ("GET", Route::Cost) => unary(core::events::cost(state, &id).await),
        ("GET", Route::Status) => unary(core::control::status(state, &id).await),
        ("GET", Route::Wait) => {
            let since = qget(query, "since").and_then(|s| s.parse().ok()).unwrap_or(0);
            let timeout =
                crate::core::control::wait_defaults(qget(query, "timeout").and_then(|s| s.parse().ok()));
            unary(core::control::wait(state, &id, since, timeout).await)
        }
        ("GET", Route::Pane) => unary(core::control::pane(state, &id).await),
        ("POST", Route::PermissionResolve) => {
            let b = parse_body(body);
            let behavior = b.get("behavior").and_then(|v| v.as_str()).unwrap_or("");
            let message = b
                .get("message")
                .and_then(|v| v.as_str())
                .map(String::from);
            unary(core::perms::resolve(state, &req_id, behavior, message).await)
        }
        ("GET", Route::Capabilities) => unary(core::capabilities::get(state, &id).await),
        ("GET", Route::EventsStream) | ("GET", Route::MessagesStream) => {
            // Guard existence; on a missing session emit one error then end.
            if crate::events_store::get_session(&state.db, &id)
                .await
                .ok()
                .flatten()
                .is_none()
            {
                return unary(Err(CoreError::not_found(&id)));
            }
            let since = qget(query, "since").and_then(|s| s.parse().ok()).unwrap_or(0);
            let transcript_only = route == Route::MessagesStream;
            let s = core::events::stream_rows(state.clone(), id, transcript_only, since)
                .map(|row| core::events::sse_frame(&row));
            Dispatched::Stream(Box::pin(s))
        }
        _ => not_found(method, path),
    }
}

fn poll_params(query: &str) -> (i64, u64) {
    let since = qget(query, "since").and_then(|s| s.parse().ok()).unwrap_or(0);
    let wait = qget(query, "wait")
        .and_then(|s| s.parse().ok())
        .unwrap_or(core::events::DEFAULT_WAIT);
    (since, wait)
}

fn qget(query: &str, key: &str) -> Option<String> {
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
    fn routes_resolve() {
        let r = routes();
        assert_eq!(*r.at("/projects").unwrap().value, Route::Projects);
        let m = r.at("/sessions/abc/status").unwrap();
        assert_eq!(*m.value, Route::Status);
        assert_eq!(m.params.get("id"), Some("abc"));
        let m = r.at("/sessions/x/permission_requests/req-1").unwrap();
        assert_eq!(*m.value, Route::PermissionResolve);
        assert_eq!(m.params.get("req_id"), Some("req-1"));
        // static beats param: /messages/stream is its own route
        assert_eq!(
            *r.at("/sessions/x/messages/stream").unwrap().value,
            Route::MessagesStream
        );
        assert!(r.at("/nope").is_err());
    }

    #[test]
    fn query_parsing() {
        assert_eq!(qget("since=5&wait=0", "since"), Some("5".into()));
        assert_eq!(poll_params("since=7&wait=10"), (7, 10));
        assert_eq!(poll_params(""), (0, core::events::DEFAULT_WAIT));
    }

    // --- parity tests (ported from the deleted http integration tests): these
    // drive the same core functions the old Axum handlers did, now via dispatch.

    use crate::auth::Token;
    use crate::config::{Config, Project};
    use crate::state::AppState;

    async fn test_state() -> AppState {
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
            std::env::temp_dir().join(format!("po-k-disp-test-{}.db", uuid::Uuid::new_v4()));
        let db = crate::events_store::open(&db_path).await.unwrap();
        AppState::new(
            Token::__test_new("t".into()),
            cfg,
            std::path::PathBuf::from("/dev/null"),
            db,
        )
    }

    async fn seed(state: &AppState, sid: &str) {
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

    fn body(d: &Dispatched) -> (u16, Value) {
        match d {
            Dispatched::Unary { status, body } => {
                (*status, serde_json::from_str(body).unwrap())
            }
            Dispatched::Stream(_) => panic!("expected unary"),
        }
    }

    #[tokio::test]
    async fn status_404_for_unknown() {
        let st = test_state().await;
        let (status, _) = body(&dispatch(&st, "GET", "/sessions/nope/status", None).await);
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn status_idle_for_fresh_session() {
        let st = test_state().await;
        seed(&st, "s1").await;
        let (status, b) = body(&dispatch(&st, "GET", "/sessions/s1/status", None).await);
        assert_eq!(status, 200);
        assert_eq!(b["status"], "idle");
        assert_eq!(b["cursor"], 0);
    }

    #[tokio::test]
    async fn wait_returns_when_ended() {
        let st = test_state().await;
        seed(&st, "s2").await;
        crate::events_store::mark_session_ended(&st.db, "s2", "2026-05-27T01:00:00Z")
            .await
            .unwrap();
        let (status, b) =
            body(&dispatch(&st, "GET", "/sessions/s2/wait?since=0&timeout=10", None).await);
        assert_eq!(status, 200);
        assert_eq!(b["status"], "ended");
    }

    #[tokio::test]
    async fn messages_poll_filters_transcript() {
        let st = test_state().await;
        seed(&st, "s3").await;
        for kind in ["user_prompt", "notification", "assistant_message", "permission_request"] {
            crate::events_store::append_event(&st.db, "s3", "t", kind, &json!({}))
                .await
                .unwrap();
        }
        let (status, b) =
            body(&dispatch(&st, "GET", "/sessions/s3/messages?since=0&wait=0", None).await);
        assert_eq!(status, 200);
        let kinds: Vec<&str> = b["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, vec!["user_prompt", "assistant_message"]);
    }

    #[tokio::test]
    async fn create_unknown_project_404() {
        let st = test_state().await;
        let (status, _) = body(
            &dispatch(&st, "POST", "/sessions", Some(r#"{"project":"nope"}"#)).await,
        );
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn projects_lists_configured() {
        let st = test_state().await;
        let (status, b) = body(&dispatch(&st, "GET", "/projects", None).await);
        assert_eq!(status, 200);
        assert_eq!(b.as_array().unwrap()[0]["name"], "po-k");
    }

    #[tokio::test]
    async fn unknown_route_404() {
        let st = test_state().await;
        let (status, _) = body(&dispatch(&st, "GET", "/nope", None).await);
        assert_eq!(status, 404);
    }
}
