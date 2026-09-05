//! The route table: the single source of truth for the axum router *and* for
//! `GET /docs`. Adding an endpoint means adding one row here; the tests below
//! check every row resolves and is described in `help.md`.

use axum::routing::{any, delete, get, post, MethodRouter};

use super::{control, docs, events, health, help, hooks_in, hub, messages, perms, sessions};
use crate::state::AppState;

pub struct ParamDoc {
    pub name: &'static str,
    pub kind: &'static str,
    pub required: bool,
    pub doc: &'static str,
}

pub struct RouteDoc {
    /// `GET`, `POST`, `DELETE`, or `ANY` (the proxy).
    pub method: &'static str,
    pub path: &'static str,
    /// Requires `Authorization: Bearer <token>`.
    pub auth: bool,
    pub summary: &'static str,
    pub query: &'static [ParamDoc],
    /// Key into `/docs` `schemas` for the request body.
    pub body: Option<&'static str>,
    pub response: &'static str,
    pub mount: fn() -> MethodRouter<AppState>,
}

const PAGE_QUERY: &[ParamDoc] = &[
    ParamDoc { name: "offset", kind: "integer", required: true, doc: "return rows with seq > offset; -1 = the latest `size` rows (tail)" },
    ParamDoc { name: "size", kind: "integer", required: true, doc: "max rows, 1..=1000" },
    ParamDoc { name: "wait", kind: "integer", required: false, doc: "long-poll seconds when the page is empty (default 30, max 60)" },
    ParamDoc { name: "follow", kind: "0|1", required: false, doc: "with offset=-1: pin to the current cursor and long-poll for NEW rows only" },
];
const SINCE_QUERY: &[ParamDoc] = &[ParamDoc { name: "since", kind: "integer", required: false, doc: "resume after this seq (default 0)" }];
const WAIT_QUERY: &[ParamDoc] = &[
    ParamDoc { name: "since", kind: "integer", required: false, doc: "the BOUNDARY cursor to wait past: `cursor` from POST /messages or `boundary_cursor` from /status. Default 0 = any past boundary satisfies" },
    ParamDoc { name: "timeout", kind: "integer", required: false, doc: "seconds (default 60, max 600); on expiry returns 200 with timed_out=true" },
];
const WATCH_QUERY: &[ParamDoc] = &[
    ParamDoc { name: "host", kind: "string", required: false, doc: "filter by host key" },
    ParamDoc { name: "state", kind: "string", required: false, doc: "active | done | failed | stopped" },
];

pub static ROUTES: &[RouteDoc] = &[
    // ---- public ----
    RouteDoc { method: "GET", path: "/health", auth: false, summary: "liveness: version + counts", query: &[], body: None, response: "{ok, version, sessions, hosts, watches}", mount: || get(health::handler) },
    RouteDoc { method: "GET", path: "/help", auth: false, summary: "this API reference as Markdown (JSON wrapper with Accept: application/json)", query: &[], body: None, response: "text/plain markdown | {format, version, content}", mount: || get(help::handler) },
    RouteDoc { method: "GET", path: "/docs", auth: false, summary: "machine-readable API description: routes, JSON Schemas, defaults, webhook contract", query: &[], body: None, response: "{service, version, auth, defaults, routes[], schemas{}, ...}", mount: || get(docs::handler) },
    // ---- local sessions ----
    RouteDoc { method: "POST", path: "/sessions", auth: true, summary: "start a Claude Code session in a directory", query: &[], body: Some("create_session"), response: "201 session | 400 {error} | 409 {error, session_id}", mount: || post(sessions::create) },
    RouteDoc { method: "GET", path: "/sessions", auth: true, summary: "list running sessions", query: &[], body: None, response: "[session]", mount: || get(sessions::list) },
    RouteDoc { method: "GET", path: "/sessions/{id}", auth: true, summary: "one running session", query: &[], body: None, response: "session | 404", mount: || get(sessions::detail) },
    RouteDoc { method: "DELETE", path: "/sessions/{id}", auth: true, summary: "stop CC and remove its zellij session", query: &[], body: None, response: "{ok, session_id}", mount: || delete(sessions::delete) },
    RouteDoc { method: "POST", path: "/sessions/{id}/messages", auth: true, summary: "type a prompt into CC; returns the boundary cursor to arm /wait with", query: &[], body: Some("send_message"), response: "{ok, bytes, cursor}", mount: || post(messages::message) },
    RouteDoc { method: "GET", path: "/sessions/{id}/messages", auth: true, summary: "transcript page (user_prompt, assistant_message, tool_use, tool_result, turn_end)", query: PAGE_QUERY, body: None, response: "{messages[], next_cursor}", mount: || get(events::messages_poll) },
    RouteDoc { method: "GET", path: "/sessions/{id}/messages/stream", auth: true, summary: "transcript as Server-Sent Events", query: SINCE_QUERY, body: None, response: "text/event-stream", mount: || get(events::messages_stream) },
    RouteDoc { method: "POST", path: "/sessions/{id}/interrupt", auth: true, summary: "send ESC (interrupt the current turn)", query: &[], body: None, response: "{ok}", mount: || post(messages::interrupt) },
    RouteDoc { method: "POST", path: "/sessions/{id}/clear", auth: true, summary: "send /clear (reset CC context)", query: &[], body: None, response: "{ok}", mount: || post(messages::clear) },
    RouteDoc { method: "POST", path: "/sessions/{id}/files", auth: true, summary: "drop a file into <cwd>/.po-k-inbox/", query: &[], body: Some("upload_file"), response: "{ok, path, bytes}", mount: || post(messages::upload_file) },
    RouteDoc { method: "GET", path: "/sessions/{id}/events", auth: true, summary: "all events page (lifecycle, hooks, transcript, permissions)", query: PAGE_QUERY, body: None, response: "{events[], next_cursor}", mount: || get(events::poll) },
    RouteDoc { method: "GET", path: "/sessions/{id}/events/stream", auth: true, summary: "all events as Server-Sent Events", query: SINCE_QUERY, body: None, response: "text/event-stream", mount: || get(events::stream) },
    RouteDoc { method: "GET", path: "/sessions/{id}/cost", auth: true, summary: "token + cost totals from turn_end events", query: &[], body: None, response: "{session_id, total_cost_usd, input_tokens, output_tokens, ...}", mount: || get(events::cost) },
    RouteDoc { method: "GET", path: "/sessions/{id}/status", auth: true, summary: "derived status + tail cursor + boundary cursor", query: &[], body: None, response: "{session_id, status, cursor, boundary_cursor, deciding_event, ended_at}", mount: || get(control::status) },
    RouteDoc { method: "GET", path: "/sessions/{id}/wait", auth: true, summary: "block until CC reaches a turn boundary newer than `since`", query: WAIT_QUERY, body: None, response: "{session_id, status, cursor, boundary_cursor, deciding_event, timed_out?}", mount: || get(control::wait) },
    RouteDoc { method: "GET", path: "/sessions/{id}/pane", auth: true, summary: "raw zellij pane content (ground truth)", query: &[], body: None, response: "{session_id, zellij_session, shows_prompt, content}", mount: || get(control::pane) },
    RouteDoc { method: "GET", path: "/sessions/{id}/capabilities", auth: true, summary: "what the session has: plugins (agents, skills, MCP), settings", query: &[], body: None, response: "{session_id, name, plugins[], capabilities{}, warnings[]}", mount: || get(sessions::capabilities) },
    RouteDoc { method: "POST", path: "/sessions/{id}/permission_requests/{req_id}", auth: true, summary: "answer a permission_request event", query: &[], body: Some("permission_decision"), response: "{ok, request_id} | 404", mount: || post(perms::resolve) },
    RouteDoc { method: "POST", path: "/sessions/{id}/hooks/{event}", auth: true, summary: "internal: CC hook callback (curl from hooks.json)", query: &[], body: None, response: "{ok, seq}", mount: || post(hooks_in::ingest) },
    RouteDoc { method: "POST", path: "/sessions/{id}/mcp/approve", auth: true, summary: "internal: blocking permission decision for `po-k cc-mcp`", query: &[], body: None, response: "{behavior, message?}", mount: || post(perms::approve) },
    // ---- hub: remote po-ks ----
    RouteDoc { method: "POST", path: "/hosts", auth: true, summary: "connect a remote po-k (probe it, remember it, optional default webhook + meta)", query: &[], body: Some("connect_host"), response: "{host, base_url, version, sessions[], webhook, meta} | 502", mount: || post(hub::connect) },
    RouteDoc { method: "GET", path: "/hosts", auth: true, summary: "connected hosts", query: &[], body: None, response: "[{host, base_url, webhook, meta, last_seen_at, last_error, active_watches}]", mount: || get(hub::list_hosts) },
    RouteDoc { method: "GET", path: "/hosts/{host}", auth: true, summary: "one host with a live probe", query: &[], body: None, response: "{host, ..., probe: {ok, version, sessions[] | error}}", mount: || get(hub::get_host) },
    RouteDoc { method: "DELETE", path: "/hosts/{host}", auth: true, summary: "forget a host and stop its watches", query: &[], body: None, response: "{ok, host, watches_stopped}", mount: || delete(hub::delete_host) },
    RouteDoc { method: "ANY", path: "/hosts/{host}/sessions", auth: true, summary: "proxy: the remote host's POST/GET /sessions. POST may add `wake`, `webhook`, `meta` to also start a watch", query: &[], body: Some("create_session"), response: "the remote response (+ `watch` on a woken create)", mount: || any(hub::proxy) },
    RouteDoc { method: "ANY", path: "/hosts/{host}/sessions/{*rest}", auth: true, summary: "proxy: any /sessions/{id}/... route on the remote host, path + query + body verbatim", query: &[], body: None, response: "the remote response | 502 {error} when unreachable", mount: || any(hub::proxy) },
    RouteDoc { method: "POST", path: "/watches", auth: true, summary: "watch a remote session: webhook on finished / needs_input / ended / connection_lost", query: &[], body: Some("create_watch"), response: "201 watch | 409 {error, watch_id}", mount: || post(hub::create_watch) },
    RouteDoc { method: "GET", path: "/watches", auth: true, summary: "list watches", query: WATCH_QUERY, body: None, response: "[watch]", mount: || get(hub::list_watches) },
    RouteDoc { method: "GET", path: "/watches/{id}", auth: true, summary: "one watch", query: &[], body: None, response: "watch | 404", mount: || get(hub::get_watch) },
    RouteDoc { method: "DELETE", path: "/watches/{id}", auth: true, summary: "stop a watch", query: &[], body: None, response: "{ok, watch_id}", mount: || delete(hub::delete_watch) },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::test_support::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn concrete(path: &str) -> String {
        path.replace("{id}", "x")
            .replace("{req_id}", "r")
            .replace("{event}", "Stop")
            .replace("{host}", "h")
            .replace("{*rest}", "x/status")
    }

    #[tokio::test]
    async fn every_route_resolves_with_the_right_auth() {
        let app = crate::http::router(test_state().await);
        for r in ROUTES {
            let method = if r.method == "ANY" { "GET" } else { r.method };
            let uri = concrete(r.path);
            let resp = app
                .clone()
                .oneshot(Request::builder().method(method).uri(&uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = resp.status();
            let v = body_json(resp).await;
            assert!(v.get("hint").is_none(), "{method} {uri} hit the 404 fallback: {v}");
            if r.auth {
                assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
            } else {
                assert_eq!(status, StatusCode::OK, "{method} {uri}");
            }
        }
    }

    #[test]
    fn help_mentions_every_route() {
        for r in ROUTES {
            let needle = format!("{} {}", r.method, r.path);
            assert!(crate::http::help::HELP_MD.contains(&needle), "help.md lacks {needle:?}");
        }
        assert!(crate::http::help::HELP_MD.contains("# po-k HTTP API"));
    }

    #[test]
    fn paths_are_unique_per_method() {
        let mut seen = std::collections::HashSet::new();
        for r in ROUTES {
            assert!(seen.insert((r.method, r.path)), "duplicate {} {}", r.method, r.path);
        }
    }
}
