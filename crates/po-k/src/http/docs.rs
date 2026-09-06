//! `GET /docs` — the machine-readable API description an agent reads before
//! it creates sessions: every route (from the same table the router is built
//! from), JSON Schemas for every request body, the defaults po-k fills in,
//! the cursor rules and the webhook contract.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use super::routes::ROUTES;
use crate::defaults;
use crate::state::AppState;

pub async fn handler(State(state): State<AppState>) -> Json<Value> {
    Json(build(&state))
}

fn schema<T: schemars::JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).unwrap_or(json!({}))
}

pub fn build(state: &AppState) -> Value {
    let routes: Vec<Value> = ROUTES
        .iter()
        .map(|r| {
            json!({
                "method": r.method,
                "path": r.path,
                "auth": r.auth,
                "summary": r.summary,
                "query": r.query.iter().map(|q| json!({ "name": q.name, "type": q.kind, "required": q.required, "doc": q.doc })).collect::<Vec<_>>(),
                "body": r.body,
                "response": r.response,
            })
        })
        .collect();
    json!({
        "service": "po-k",
        "version": env!("CARGO_PKG_VERSION"),
        "bind": state.config.server.bind,
        "auth": {
            "scheme": "bearer",
            "header": "Authorization: Bearer <token>",
            "public_routes": ["/health", "/help", "/docs"],
            "note": "one fleet-wide token; the same token is accepted by every po-k and used by the hub to call remote ones",
        },
        "version_handshake": {
            "header": crate::version::HEADER,
            "rule": "every po-k → po-k request carries this header; a different version is refused with 409 {error, server_version, client_version}. POST /hosts also compares the remote /health version and answers 409 on mismatch.",
        },
        "defaults": {
            "model": defaults::MODEL,
            "effort": defaults::EFFORT,
            "permission_mode": defaults::PERMISSION_MODE,
            "permission_modes": defaults::PERMISSION_MODES,
            "permission_timeout_secs": defaults::PERMISSION_TIMEOUT.as_secs(),
            "disable_slash_commands": defaults::DISABLE_SLASH_COMMANDS,
            "zellij_session_prefix": defaults::ZELLIJ_SESSION_PREFIX,
            "remote_port": defaults::remote_port(),
            "host_suffix": defaults::host_suffix(),
        },
        "routes": routes,
        "schemas": {
            "create_session": crate::core::sessions::CreateRequest::json_schema(),
            "send_message": schema::<super::messages::MessageBody>(),
            "upload_file": schema::<super::messages::FileBody>(),
            "permission_decision": schema::<super::perms::ResolveBody>(),
            "connect_host": schema::<super::hub::ConnectBody>(),
            "create_watch": schema::<super::hub::WatchBody>(),
            "webhook_event": webhook_event_schema(),
        },
        "session_status_values": ["working", "awaiting_input", "idle", "ended"],
        "event_kinds": {
            "lifecycle": ["cc_started", "cc_exited", "cc_recovered", "cc_lost"],
            "hooks": ["user_prompt", "stop", "subagent_stop", "tool_result", "notification", "idle_notification", "session_end"],
            "transcript": ["user_prompt", "assistant_message", "tool_use", "tool_result", "user_question", "turn_end", "raw_<type>"],
            "permissions": ["permission_request", "permission_decision"],
        },
        "cursor_rules": [
            "tail cursor = `cursor` on /status and /wait, `next_cursor` on /events: the highest seq stored; use it to page forward",
            "boundary cursor = `boundary_cursor` on /status and /wait, `cursor` from POST /messages: the seq of the deciding stop/notification event; ONLY this is valid as /wait?since=",
            "never re-arm /wait with next_cursor from /events — the tail is usually higher than the boundary and the wait would block until the NEXT turn",
            "read the final transcript with wait>=2 after /wait returns: the Stop hook lands before the tailer flushes the last assistant_message",
        ],
        "webhook": {
            "events": ["finished", "needs_input", "ended", "connection_lost", "connection_restored", "session_lost", "auth_failed", "version_mismatch"],
            "headers": { "x-webhook-signature": "hex HMAC-SHA256 of the exact body under the secret", "x-request-id": "<watch_id>:<event>:<boundary_cursor> (idempotency key)", "x-pok-event": "pok_notification" },
            "body_is_metadata_only": true,
            "secret": "referenced by env var name (`secret_env`, read from the `po-k serve` environment) or file path (`secret_file`); never stored or echoed",
        },
        "quickstart": [
            "POST /hosts {\"host\": \"<box>\", \"webhook\": {\"url\": \"http://127.0.0.1:8644/webhooks/pok\", \"secret_env\": \"POK_WEBHOOK_SECRET\"}}",
            "POST /hosts/<box>/sessions {\"cwd\": \"/workspace\", \"model\": \"fable\", \"plugins\": [\"/zirzen/base/plugins/sapi\"], \"wake\": true}  → 201 {session_id, watch}",
            "POST /hosts/<box>/sessions/<sid>/messages {\"text\": \"...\"}  → {cursor}",
            "either wait for the webhook, or GET /hosts/<box>/sessions/<sid>/wait?since=<cursor>&timeout=600",
            "GET /hosts/<box>/sessions/<sid>/messages?offset=-1&size=10&wait=2",
            "DELETE /hosts/<box>/sessions/<sid>",
        ],
    })
}

fn webhook_event_schema() -> Value {
    json!({
        "type": "object",
        "description": "POSTed by the hub to the watch's webhook URL on every turn boundary and connectivity change. Metadata only — fetch content with GET .../events.",
        "properties": {
            "event_type": { "const": "pok_notification" },
            "event": { "enum": ["finished", "needs_input", "ended", "connection_lost", "connection_restored", "session_lost", "auth_failed", "version_mismatch"] },
            "host": { "type": "string" },
            "session_id": { "type": "string" },
            "watch_id": { "type": "string" },
            "status": { "type": ["string", "null"], "enum": ["working", "awaiting_input", "idle", "ended", null] },
            "boundary_cursor": { "type": "integer", "description": "arm the next /wait with this" },
            "deciding_event": { "type": ["object", "null"], "properties": { "kind": {}, "seq": {}, "ts": {}, "payload": { "description": "present for user_question / permission_request" } } },
            "message": { "type": ["string", "null"], "description": "human-readable detail for connection/auth events" },
            "meta": { "description": "whatever was attached to the host/watch (e.g. chat routing ids)" },
            "origin": { "type": "object", "description": "templating-safe projection of meta: platform, chat_id, chat_name, thread_id, user_id, user_name, session_key, hint — always present, empty string when unknown" },
            "ts": { "type": "string" }
        },
        "required": ["event_type", "event", "host", "session_id", "watch_id", "boundary_cursor", "ts"]
    })
}

#[cfg(test)]
mod tests {
    use crate::http::test_support::*;

    #[tokio::test]
    async fn docs_list_every_route_and_carry_schemas() {
        let st = test_state().await;
        let v = super::build(&st);
        let paths: Vec<String> = v["routes"].as_array().unwrap().iter().map(|r| format!("{} {}", r["method"].as_str().unwrap(), r["path"].as_str().unwrap())).collect();
        assert_eq!(paths.len(), super::ROUTES.len());
        assert!(paths.contains(&"POST /sessions".to_string()));
        assert!(paths.contains(&"ANY /hosts/{host}/sessions/{*rest}".to_string()));
        let schemas = v["schemas"].as_object().unwrap();
        for key in ["create_session", "send_message", "upload_file", "permission_decision", "connect_host", "create_watch", "webhook_event"] {
            assert!(schemas.contains_key(key), "missing schema {key}");
        }
        assert_eq!(v["schemas"]["create_session"]["required"], serde_json::json!(["cwd"]));
        assert_eq!(v["defaults"]["model"], "fable");
        // Every route body key refers to an existing schema.
        for r in super::ROUTES {
            if let Some(b) = r.body {
                assert!(schemas.contains_key(b), "route {} {} references unknown schema {b}", r.method, r.path);
            }
        }
    }
}
