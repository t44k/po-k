//! Health + registry views (Xpo-k's own, not routed to po-k).

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::state::XState;

pub async fn health(State(st): State<XState>) -> Json<Value> {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "connected_pok": st.registry.connected_count(),
    }))
}

pub async fn registry(State(st): State<XState>) -> Json<Value> {
    Json(st.registry.list())
}

/// `GET /clients` — lightweight view of connected po-k instances.
pub async fn clients(State(st): State<XState>) -> Json<Value> {
    Json(st.registry.list_clients())
}

/// `GET /help` — public, unauthenticated API reference. Lists every route with
/// its method, auth requirement, and a one-line description so new users and
/// tools can discover the API without external docs.
pub async fn help() -> Json<Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "endpoints": [
            // --- public ---
            {
                "method": "GET",
                "path": "/health",
                "auth": false,
                "description": "Connectivity check. Returns version and connected po-k count."
            },
            {
                "method": "GET",
                "path": "/help",
                "auth": false,
                "description": "This endpoint. Returns API reference."
            },
            // --- registry / clients ---
            {
                "method": "GET",
                "path": "/registry",
                "auth": true,
                "description": "Full registry view of connected po-k instances and their projects."
            },
            {
                "method": "GET",
                "path": "/clients",
                "auth": true,
                "description": "Lightweight view of connected po-k instances (pok_id, hostname, version, ad_hoc, project count)."
            },
            // --- profiles ---
            {
                "method": "GET",
                "path": "/profiles",
                "auth": true,
                "description": "List all profiles (summaries)."
            },
            {
                "method": "POST",
                "path": "/profiles",
                "auth": true,
                "description": "Create a profile. Body: profile object with required string `name` (fields: claude_md, agents, skills, mcp_servers, hooks, settings, tags)."
            },
            {
                "method": "GET",
                "path": "/profiles/{name}",
                "auth": true,
                "description": "Get a single profile by name."
            },
            {
                "method": "PUT",
                "path": "/profiles/{name}",
                "auth": true,
                "description": "Update a profile. Body: profile object; the path name is authoritative."
            },
            {
                "method": "DELETE",
                "path": "/profiles/{name}",
                "auth": true,
                "description": "Delete a profile by name."
            },
            {
                "method": "GET",
                "path": "/profiles/{name}/history",
                "auth": true,
                "description": "List the version history of a profile."
            },
            {
                "method": "POST",
                "path": "/profiles/merge",
                "auth": true,
                "description": "Merge named profiles into one. Body: {\"profiles\": [\"name\", ...]}."
            },
            {
                "method": "POST",
                "path": "/profiles/preview",
                "auth": true,
                "description": "Preview the composition of profiles without persisting. Body: {\"profiles\": [\"name\", ...]}."
            },
            // --- projects ---
            {
                "method": "GET",
                "path": "/projects",
                "auth": true,
                "description": "List projects across all connected po-k instances, each enriched with pok_id and hostname."
            },
            // --- sessions ---
            {
                "method": "GET",
                "path": "/sessions",
                "auth": true,
                "description": "List active CC sessions across all connected po-k instances, enriched with pok_id and hostname."
            },
            {
                "method": "POST",
                "path": "/sessions",
                "auth": true,
                "description": "Create a CC session. Body: {project?, cwd?, pok_id?, host?, profiles?, model?, effort?}. Routes by project, then pok_id/host, then a lone ad-hoc po-k."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}",
                "auth": true,
                "description": "Get details for a single session."
            },
            {
                "method": "DELETE",
                "path": "/sessions/{id}",
                "auth": true,
                "description": "Tear down a session (sends /exit, force-deletes the zellij session, marks ended)."
            },
            {
                "method": "POST",
                "path": "/sessions/{id}/messages",
                "auth": true,
                "description": "Send a prompt to the session. Body: {\"text\": \"...\"}. Blocks until CC's prompt is ready, returns a cursor."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/messages",
                "auth": true,
                "description": "Read messages. Required query params: offset (int), size (int). offset=-1 = tail (latest size); offset>=0 = cursor (seq > offset). size capped at 1000; missing either → 400. Optional: wait (int seconds)."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/messages/stream",
                "auth": true,
                "description": "SSE stream of messages. Optional query: since (cursor)."
            },
            {
                "method": "POST",
                "path": "/sessions/{id}/interrupt",
                "auth": true,
                "description": "Send ESC to interrupt the running session (e.g. dismiss a prompt)."
            },
            {
                "method": "POST",
                "path": "/sessions/{id}/clear",
                "auth": true,
                "description": "Send /clear to reset the session's context."
            },
            {
                "method": "POST",
                "path": "/sessions/{id}/files",
                "auth": true,
                "description": "Upload a file to the session's .po-k-inbox/. Body: {\"filename\": \"...\", \"content_base64\": \"...\"}."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/events",
                "auth": true,
                "description": "Read events. Required query params: offset (int), size (int). offset=-1 = tail (latest size); offset>=0 = cursor (seq > offset). size capped at 1000; missing either → 400. Optional: wait (int seconds)."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/events/stream",
                "auth": true,
                "description": "SSE stream of events. Optional query: since (cursor)."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/cost",
                "auth": true,
                "description": "Get token usage and cost totals for the session."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/status",
                "auth": true,
                "description": "Get the session's current status: working, idle, awaiting_input, or ended."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/wait",
                "auth": true,
                "description": "Block until the session becomes idle/awaiting_input/ended. Optional query: since (cursor), timeout (int seconds, max 600)."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/pane",
                "auth": true,
                "description": "Read the raw terminal pane content (what's visible on screen)."
            },
            {
                "method": "GET",
                "path": "/sessions/{id}/capabilities",
                "auth": true,
                "description": "Get the agents, skills, and MCP servers available in the session."
            },
            {
                "method": "POST",
                "path": "/sessions/{id}/permission_requests/{req_id}",
                "auth": true,
                "description": "Answer a pending MCP permission request. Body: {\"behavior\": \"allow\"|\"deny\", \"message?\": \"...\"}."
            }
        ]
    }))
}
