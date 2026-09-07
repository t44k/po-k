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
            "send_keys": schema::<super::messages::KeysBody>(),
            "webhook_event": webhook_event_schema(),
            "notification": notification_schema(),
        },
        "session_status_values": ["working", "awaiting_input", "idle", "ended"],
        "event_kinds": {
            "lifecycle": ["cc_started", "cc_exited", "cc_recovered", "cc_lost"],
            "hooks": ["user_prompt", "stop", "subagent_stop", "tool_result", "notification", "idle_notification", "permission_prompt", "session_end"],
            "transcript": ["user_prompt", "assistant_message", "tool_use", "tool_result", "user_question", "turn_end", "raw_<type>"],
            "permissions": ["permission_request", "permission_decision"],
        },
        "cursor_rules": [
            "tail cursor = `cursor` on /status and /wait, `next_cursor` on /events: the highest seq stored; use it to page forward",
            "boundary cursor = `boundary_cursor` on /status and /wait, `cursor` from POST /messages: the seq of the deciding stop/notification event; ONLY this is valid as /wait?since=",
            "never re-arm /wait with next_cursor from /events — the tail is usually higher than the boundary and the wait would block until the NEXT turn",
            "read the final transcript with wait>=2 after /wait returns: the Stop hook lands before the tailer flushes the last assistant_message",
        ],
        "input_recipes": {
            "permission_request": "POST /sessions/{id}/permission_requests/{req_id} {behavior}",
            "permission_prompt": "CC's native TUI picker (deciding_event.payload has `pane` and `options`): POST /sessions/{id}/keys {\"keys\": [\"<option index>\", \"enter\"]}",
            "user_question": "POST /sessions/{id}/messages {text} — or /keys when the question is a picker",
        },
        "routing_meta": {
            "rule": "a watch (wake / webhook / POST /watches) needs routing metadata: platform, chat_id, thread_id (non-empty strings) in meta, or inherited from the host's meta (explicit keys overlay the host's); otherwise 400",
            "zulip": { "platform": "zulip", "chat_id": "stream:<stream>", "thread_id": "<topic>" },
            "escape_hatch": { "unrouted": true },
            "max_bytes": defaults::META_MAX_BYTES,
        },
        "webhook": {
            "events": ["finished", "needs_input", "ended", "connection_lost", "connection_restored", "session_lost", "auth_failed", "version_mismatch"],
            "headers": { "x-webhook-signature": "hex HMAC-SHA256 of the exact body under the secret", "x-request-id": "<notification_id>:<attempt> (fresh per attempt, so a replay is not deduped away)", "x-pok-event": "pok_notification" },
            "body_is_metadata_only": true,
            "secret": "referenced by env var name (`secret_env`, read from the `po-k serve` environment) or file path (`secret_file`); never stored or echoed",
            "loop": "boundary → notification row (exactly one per watch/event/boundary_cursor) → POST → receiver acts → POST /notifications/{id}/ack",
            "ack_rule": "finished, needs_input, ended, session_lost require an ack (`requires_ack: true`); ack AFTER acting on it (reply posted / follow-up prompt sent / permission answered). Connectivity events auto-ack on 2xx.",
            "replay": format!("an unacked notification is POSTed again after ack_timeout_secs (default {}, per watch), then with doubling intervals up to {} s, at most {} attempts, then state=failed; `attempt` in the body increases, `notification_id` stays", defaults::ACK_TIMEOUT.as_secs(), defaults::REPLAY_MAX_INTERVAL.as_secs(), defaults::REPLAY_MAX_ATTEMPTS),
            "transport_failure": "connection refused / 5xx / 429 → retried with backoff 30 s → 15 m (state stays pending); other 4xx → state=failed with last_error (fix the route or secret)",
            "on_receipt": "if first_delivered_at is set (a replay), GET /notifications/{id} first and stop if already acked; the body lists `unacked_previous` (older notifications of the same watch still owed an ack)",
            "restart": "pending and overdue rows are delivered after `po-k serve` restarts; DELETE /watches/{id} and DELETE /hosts/{host} cancel outstanding rows",
        },
        "quickstart": [
            "POST /hosts {\"host\": \"<box>\", \"webhook\": {\"url\": \"http://127.0.0.1:8644/webhooks/pok\", \"secret_env\": \"POK_WEBHOOK_SECRET\"}}",
            "POST /hosts/<box>/sessions {\"cwd\": \"/workspace\", \"model\": \"fable\", \"plugins\": [\"/zirzen/base/plugins/sapi\"], \"wake\": true}  → 201 {session_id, watch}",
            "POST /hosts/<box>/sessions/<sid>/messages {\"text\": \"...\"}  → {cursor}",
            "the webhook fires with {notification_id, event, boundary_cursor, origin}; GET /hosts/<box>/sessions/<sid>/messages?offset=-1&size=10&wait=2 to read the reply",
            "act on it, then POST /notifications/<notification_id>/ack (otherwise it is re-sent after ack_timeout_secs)",
            "short tasks only: GET /hosts/<box>/sessions/<sid>/wait?since=<cursor>&timeout=120",
            "DELETE /hosts/<box>/sessions/<sid>",
        ],
    })
}

fn webhook_event_schema() -> Value {
    json!({
        "type": "object",
        "description": "POSTed by the hub to the watch's webhook URL on every turn boundary and connectivity change, and again while unacknowledged. Metadata only — fetch content with GET .../events.",
        "properties": {
            "event_type": { "const": "pok_notification" },
            "notification_id": { "type": "string", "description": "stable across replays; POST /notifications/{id}/ack when handled" },
            "attempt": { "type": "integer", "description": "POST attempts so far including this one; when first_delivered_at is set this is a replay of a still-unacked notification — check GET /notifications/{id} before acting" },
            "first_delivered_at": { "type": ["string", "null"] },
            "requires_ack": { "type": "boolean" },
            "unacked_previous": { "type": "array", "items": { "type": "string" }, "description": "older notifications of the same watch still owed an ack" },
            "event": { "enum": ["finished", "needs_input", "ended", "connection_lost", "connection_restored", "session_lost", "auth_failed", "version_mismatch"] },
            "host": { "type": "string" },
            "session_id": { "type": "string" },
            "watch_id": { "type": "string" },
            "status": { "type": ["string", "null"], "enum": ["working", "awaiting_input", "idle", "ended", null] },
            "boundary_cursor": { "type": "integer", "description": "arm the next /wait with this" },
            "deciding_event": { "type": ["object", "null"], "properties": { "kind": {}, "seq": {}, "ts": {}, "payload": { "description": "present for user_question / permission_request / permission_prompt (pane + options)" } } },
            "message": { "type": ["string", "null"], "description": "human-readable detail for connection/auth events" },
            "origin": { "type": "object", "description": "templating-safe projection of the watch's meta: platform, chat_id, chat_name, thread_id, user_id, user_name, session_key, hint — always present, empty string when unknown" },
            "ts": { "type": "string" }
        },
        "required": ["event_type", "notification_id", "attempt", "event", "host", "session_id", "watch_id", "boundary_cursor", "requires_ack", "ts"]
    })
}

fn notification_schema() -> Value {
    json!({
        "type": "object",
        "description": "A row of the hub's notification log (GET /notifications). One per watch/event/boundary_cursor.",
        "properties": {
            "id": { "type": "string" },
            "watch_id": { "type": "string" },
            "host": { "type": "string" },
            "session_id": { "type": "string" },
            "event": { "type": "string" },
            "boundary_cursor": { "type": "integer" },
            "status": { "type": ["string", "null"] },
            "deciding_event": { "type": ["object", "null"] },
            "message": { "type": ["string", "null"] },
            "origin": { "type": ["object", "null"] },
            "requires_ack": { "type": "boolean" },
            "state": { "enum": ["pending", "delivered", "acked", "failed", "cancelled"], "description": "pending = not yet accepted by the receiver; delivered = accepted, ack outstanding (replayed after ack_timeout); acked = done; failed = gave up (4xx or too many replays); cancelled = watch stopped" },
            "attempts": { "type": "integer" },
            "next_attempt_at": { "type": ["string", "null"] },
            "first_delivered_at": { "type": ["string", "null"] },
            "delivered_at": { "type": ["string", "null"] },
            "acked_at": { "type": ["string", "null"] },
            "last_error": { "type": ["string", "null"] },
            "created_at": { "type": "string" },
            "updated_at": { "type": "string" }
        }
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
        for key in ["create_session", "send_message", "upload_file", "permission_decision", "connect_host", "create_watch", "send_keys", "webhook_event", "notification"] {
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
