//! Bundled MCP server: JSON-RPC 2.0 over a single POST /mcp endpoint.
//!
//! v1 implements just enough of the protocol to be useful from Claude Code's
//! MCP client: `initialize`, `tools/list`, `tools/call`. SSE / streamed responses
//! aren't needed since every tool returns a single JSON payload.
//!
//! Auth: `X-Api-Key` is required; the tool results are scoped to the key's `team_id`.
//! A missing or unknown key returns a JSON-RPC error (so MCP clients see a usable
//! protocol-level error instead of a 401 HTML body).

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use po_k_proto::HEADER_API_KEY;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::auth;
use crate::search;
use crate::state::AppState;
use crate::topics;

const PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Debug, Deserialize)]
struct Request {
    #[serde(default)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Resolve team scope from the API key. Notifications (no id) without auth are
    // tolerated so a probing client can ping and see protocol-level errors.
    let presented = headers
        .get(HEADER_API_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let auth_ctx: Option<crate::auth::AuthCtx> = match auth::lookup(state.pool(), presented).await {
        Ok(Some(ctx)) => Some(ctx),
        Ok(None) => None,
        Err(e) => {
            tracing::error!(error = %e, "mcp auth lookup failed");
            None
        }
    };

    let req: Request = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return jsonrpc_error_resp(
                None,
                -32700,
                &format!("parse error: {e}"),
                StatusCode::BAD_REQUEST,
            )
        }
    };
    if req.jsonrpc.as_deref() != Some("2.0") {
        return jsonrpc_error_resp(
            req.id.clone(),
            -32600,
            "invalid request: jsonrpc must be \"2.0\"",
            StatusCode::OK,
        );
    }

    let id = req.id.clone();
    match req.method.as_str() {
        "initialize" => jsonrpc_ok(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "po-k",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
        ),
        // Common no-op notifications / pings we want to ack cleanly.
        "notifications/initialized" | "ping" => jsonrpc_ok(id, json!({})),
        "tools/list" => jsonrpc_ok(id, json!({ "tools": tool_definitions() })),
        "tools/call" => match auth_ctx.as_ref() {
            Some(ctx) => {
                let resp = handle_tool_call(&state, ctx, &req.params).await;
                match resp {
                    Ok(v) => jsonrpc_ok(id, v),
                    Err((code, msg)) => jsonrpc_error_resp(id, code, &msg, StatusCode::OK),
                }
            }
            None => jsonrpc_error_resp(
                id,
                -32001,
                "missing or invalid X-Api-Key (tools/call requires auth)",
                StatusCode::OK,
            ),
        },
        other => jsonrpc_error_resp(
            id,
            -32601,
            &format!("method not found: {other}"),
            StatusCode::OK,
        ),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "search_sessions",
            "description": "BM25 keyword search over Claude Code session events for the calling team. Returns top hits with snippets and links back to the originating session, file, and line. Use this to find prior conversations or tool calls related to a topic.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search terms (whitespace-separated)." },
                    "limit": { "type": "integer", "default": 25, "minimum": 1, "maximum": 200 }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "list_projects",
            "description": "List projects (sanitized working directories) in the calling team, with session counts and last activity timestamps.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "recent_sessions",
            "description": "List recent sessions in the calling team. Returns session_uuid, sanitized_cwd, event_count, and timestamps.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "default": 20, "minimum": 1, "maximum": 200 }
                }
            }
        }),
        json!({
            "name": "list_topics",
            "description": "List curated topics for the calling team. Each topic has an id, question, scope, the latest digest version (0 if not yet distilled), and when it was last written. Use this to discover what shared knowledge po-k maintains for this team.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "recall_topic",
            "description": "Fetch the current markdown digest for a topic — the distilled answer the team maintains across sessions. Returns the digest text, version, evidence count, and the topic question/scope. Use this BEFORE asking the user about a topic that may already have a maintained answer.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "topic_id": { "type": "string", "description": "The topic's kebab-case id (e.g. \"auth-pattern\")." }
                },
                "required": ["topic_id"]
            }
        }),
    ]
}

async fn handle_tool_call(
    state: &AppState,
    ctx: &crate::auth::AuthCtx,
    params: &Value,
) -> Result<Value, (i64, String)> {
    let team = ctx.team_id.as_str();
    let caller_user_id = ctx.user_id.as_str();
    let is_admin = ctx.role.is_admin();
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| (-32602, "missing tool name".to_string()))?;
    let args = params.get("arguments").cloned().unwrap_or(Value::Null);

    match name {
        "search_sessions" => {
            let query = args.get("query").and_then(Value::as_str).unwrap_or("");
            let limit = args
                .get("limit")
                .and_then(Value::as_i64)
                .unwrap_or(25)
                .clamp(1, 200);
            let hits = search::bm25(state.pool(), query, Some(team), limit)
                .await
                .map_err(|e| (-32603, format!("search error: {e}")))?;
            let payload =
                serde_json::to_string_pretty(&hits).unwrap_or_else(|_| "[]".to_string());
            Ok(json!({
                "content": [{ "type": "text", "text": payload }],
                "isError": false,
                "structuredContent": { "hits": hits }
            }))
        }
        "list_projects" => {
            let rows = sqlx::query(
                "SELECT sanitized_cwd,
                        COUNT(*) AS session_count,
                        COALESCE(SUM(event_count), 0) AS event_count,
                        MAX(last_event_at) AS last_event_at
                 FROM sessions WHERE team_id = ?
                 GROUP BY sanitized_cwd
                 ORDER BY MAX(last_event_at) DESC",
            )
            .bind(team)
            .fetch_all(state.pool())
            .await
            .map_err(|e| (-32603, format!("db error: {e}")))?;
            let projects: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "sanitized_cwd": r.try_get::<String, _>("sanitized_cwd").unwrap_or_default(),
                        "session_count": r.try_get::<i64, _>("session_count").unwrap_or(0),
                        "event_count": r.try_get::<i64, _>("event_count").unwrap_or(0),
                        "last_event_at": r.try_get::<Option<String>, _>("last_event_at").unwrap_or_default(),
                    })
                })
                .collect();
            let payload = serde_json::to_string_pretty(&projects).unwrap_or_default();
            Ok(json!({
                "content": [{ "type": "text", "text": payload }],
                "isError": false,
                "structuredContent": { "projects": projects }
            }))
        }
        "recent_sessions" => {
            let limit = args
                .get("limit")
                .and_then(Value::as_i64)
                .unwrap_or(20)
                .clamp(1, 200);
            let rows = sqlx::query(
                "SELECT session_key, sanitized_cwd, session_uuid, event_count, first_event_at, last_event_at
                 FROM sessions WHERE team_id = ?
                 ORDER BY last_event_at DESC NULLS LAST
                 LIMIT ?",
            )
            .bind(team)
            .bind(limit)
            .fetch_all(state.pool())
            .await
            .map_err(|e| (-32603, format!("db error: {e}")))?;
            let sessions: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "session_key": r.try_get::<String, _>("session_key").unwrap_or_default(),
                        "sanitized_cwd": r.try_get::<String, _>("sanitized_cwd").unwrap_or_default(),
                        "session_uuid": r.try_get::<String, _>("session_uuid").unwrap_or_default(),
                        "event_count": r.try_get::<i64, _>("event_count").unwrap_or(0),
                        "first_event_at": r.try_get::<Option<String>, _>("first_event_at").unwrap_or_default(),
                        "last_event_at": r.try_get::<Option<String>, _>("last_event_at").unwrap_or_default(),
                    })
                })
                .collect();
            let payload = serde_json::to_string_pretty(&sessions).unwrap_or_default();
            Ok(json!({
                "content": [{ "type": "text", "text": payload }],
                "isError": false,
                "structuredContent": { "sessions": sessions }
            }))
        }
        "list_topics" => {
            let all = topics::list_with_digests(state.pool(), team)
                .await
                .map_err(|e| (-32603, format!("topics list error: {e}")))?;
            // Members see global / global-project topics and their own user-scoped
            // ones; admin sees everything in the team.
            let filtered: Vec<_> = all
                .into_iter()
                .filter(|t| {
                    if is_admin {
                        return true;
                    }
                    match t.topic.scope_kind.as_str() {
                        "global" | "global-project" => true,
                        "user" | "user-project" => {
                            t.topic.user_id.as_deref() == Some(caller_user_id)
                        }
                        _ => false,
                    }
                })
                .collect();
            let items: Vec<Value> = filtered
                .iter()
                .map(|t| {
                    json!({
                        "id": t.topic.id,
                        "question": t.topic.question,
                        "scope_kind": t.topic.scope_kind,
                        "user_id": t.topic.user_id,
                        "project_id": t.topic.project_id,
                        "version": t.version,
                        "written_at": t.written_at,
                        "llm_backend": t.llm_backend,
                    })
                })
                .collect();
            let payload = serde_json::to_string_pretty(&items).unwrap_or_default();
            Ok(json!({
                "content": [{ "type": "text", "text": payload }],
                "isError": false,
                "structuredContent": { "topics": items }
            }))
        }
        "recall_topic" => {
            let topic_id = args
                .get("topic_id")
                .and_then(Value::as_str)
                .ok_or_else(|| (-32602, "missing topic_id".to_string()))?;
            let t = topics::get_with_digest(state.pool(), topic_id)
                .await
                .map_err(|e| (-32603, format!("db error: {e}")))?;
            let Some(t) = t else {
                return Err((-32004, format!("no topic with id '{topic_id}'")));
            };
            if t.topic.team_id != team {
                return Err((-32001, "topic belongs to a different team".to_string()));
            }
            // Members can only read their own user-scoped topics.
            let user_scoped = matches!(t.topic.scope_kind.as_str(), "user" | "user-project");
            if user_scoped
                && !is_admin
                && t.topic.user_id.as_deref() != Some(caller_user_id)
            {
                return Err((-32001, "topic belongs to a different user".to_string()));
            }
            let evidence_count = serde_json::from_str::<Vec<Value>>(&t.evidence_event_ids)
                .map(|v| v.len())
                .unwrap_or(0);
            let body = json!({
                "topic": {
                    "id": t.topic.id,
                    "question": t.topic.question,
                    "scope_kind": t.topic.scope_kind,
                    "user_id": t.topic.user_id,
                    "project_id": t.topic.project_id,
                },
                "version": t.version,
                "written_at": t.written_at,
                "evidence_count": evidence_count,
                "llm_backend": t.llm_backend,
                "llm_model": t.llm_model,
                "digest_markdown": t.digest_markdown,
            });
            Ok(json!({
                "content": [{ "type": "text", "text": t.digest_markdown }],
                "isError": false,
                "structuredContent": body
            }))
        }
        other => Err((-32601, format!("tool not found: {other}"))),
    }
}

fn jsonrpc_ok(id: Option<Value>, result: Value) -> Response {
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

fn jsonrpc_error_resp(id: Option<Value>, code: i64, message: &str, status: StatusCode) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    });
    (status, Json(body)).into_response()
}
