"""Agent-facing tool schemas and handlers for po-k / Xpo-k.

Each tool follows the Hermes plugin handler convention:
    handler(args: dict, **kwargs) -> dict | str
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

TOOLSET = "pok"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(data: Dict[str, Any]) -> str:
    return json.dumps({"success": True, **data})


def _err(msg: str) -> str:
    return json.dumps({"success": False, "error": msg})


def _client():
    """Lazy import to avoid import-time env checks."""
    from .client import get_client
    return get_client()


def _check() -> bool:
    """Check function — is XPOK_URL set?"""
    return bool(os.getenv("XPOK_URL"))


# ---------------------------------------------------------------------------
# Tool: pok_clients
# ---------------------------------------------------------------------------

POK_CLIENTS_SCHEMA = {
    "name": "pok_clients",
    "description": (
        "List connected po-k instances (Claude Code host machines). "
        "Returns pok_id, hostname, version, ad_hoc capability, and project count for each."
    ),
    "parameters": {"type": "object", "properties": {}},
}


def _handle_pok_clients(args: dict, **_kw) -> str:
    try:
        data = _client().clients()
        return _ok({"clients": data})
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_projects
# ---------------------------------------------------------------------------

POK_PROJECTS_SCHEMA = {
    "name": "pok_projects",
    "description": (
        "List projects across all connected po-k instances. "
        "Each entry includes the project name, cwd, owning pok_id and hostname."
    ),
    "parameters": {"type": "object", "properties": {}},
}


def _handle_pok_projects(args: dict, **_kw) -> str:
    try:
        data = _client().projects()
        return _ok({"projects": data})
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_sessions
# ---------------------------------------------------------------------------

POK_SESSIONS_SCHEMA = {
    "name": "pok_sessions",
    "description": (
        "List all active CC sessions across all connected po-k instances. "
        "Each entry includes session_id, project, cwd, model, effort, started_at, pok_id, hostname."
    ),
    "parameters": {"type": "object", "properties": {}},
}


def _handle_pok_sessions(args: dict, **_kw) -> str:
    try:
        data = _client().sessions()
        return _ok({"sessions": data})
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_create
# ---------------------------------------------------------------------------

POK_CREATE_SCHEMA = {
    "name": "pok_create",
    "description": (
        "Create a new Claude Code session on a po-k instance. "
        "Specify a configured project name, or use cwd for ad-hoc sessions "
        "(requires cc.ad_hoc: true on the target po-k). "
        "Route to a specific instance via host or pok_id."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "project": {
                "type": "string",
                "description": "Project name (from po-k.yaml). Can be empty for ad-hoc sessions.",
            },
            "cwd": {
                "type": "string",
                "description": "Working directory for the session. Required for ad-hoc; optional override for configured projects.",
            },
            "host": {
                "type": "string",
                "description": "Target po-k instance by hostname.",
            },
            "pok_id": {
                "type": "string",
                "description": "Target po-k instance by pok_id.",
            },
            "profiles": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Xpo-k profile names to compose for this session.",
            },
            "model": {
                "type": "string",
                "description": "CC model override (e.g. 'opus', 'sonnet').",
            },
            "effort": {
                "type": "string",
                "description": "CC effort override (e.g. 'high', 'medium').",
            },
        },
    },
}


def _handle_pok_create(args: dict, **_kw) -> str:
    try:
        data = _client().create_session(
            project=args.get("project", ""),
            cwd=args.get("cwd", ""),
            host=args.get("host", ""),
            pok_id=args.get("pok_id", ""),
            profiles=args.get("profiles"),
            model=args.get("model", ""),
            effort=args.get("effort", ""),
        )
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_prompt
# ---------------------------------------------------------------------------

POK_PROMPT_SCHEMA = {
    "name": "pok_prompt",
    "description": (
        "Send a prompt to a running CC session. Blocks until CC's input prompt is ready "
        "(up to 120s). Returns a cursor for subsequent /wait or /events calls."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "text": {"type": "string", "description": "The prompt text to send to CC."},
        },
        "required": ["session_id", "text"],
    },
}


def _handle_pok_prompt(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    text = args.get("text", "")
    if not sid or not text:
        return _err("session_id and text are required")
    try:
        data = _client().send_message(sid, text)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_status
# ---------------------------------------------------------------------------

POK_STATUS_SCHEMA = {
    "name": "pok_status",
    "description": (
        "Get the current status of a CC session: working, idle, awaiting_input, or ended."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_status(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().get_status(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_wait
# ---------------------------------------------------------------------------

POK_WAIT_SCHEMA = {
    "name": "pok_wait",
    "description": (
        "Block until a CC session becomes idle, awaiting_input, or ended. "
        "Returns the deciding event. Use 'since' cursor from pok_prompt to avoid "
        "seeing stale events. Max server-side timeout is 600s; returns timed_out: true on timeout."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "since": {
                "type": "integer",
                "description": "Cursor from pok_prompt. Only events after this seq are considered.",
            },
            "timeout": {
                "type": "integer",
                "description": "Max seconds to wait (default 600, max 600).",
            },
        },
        "required": ["session_id"],
    },
}


def _handle_pok_wait(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    since = args.get("since", 0)
    timeout = min(args.get("timeout", 600), 600)
    try:
        data = _client().wait(sid, since=since, timeout=timeout)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_events
# ---------------------------------------------------------------------------

POK_EVENTS_SCHEMA = {
    "name": "pok_events",
    "description": (
        "Fetch events from a CC session. Use to read CC's reply after /wait returns idle. "
        "Look for events with kind='stop' — the 'last_assistant_message' field contains "
        "CC's reply text.\n\n"
        "Pagination: 'offset' and 'size' control the window. offset=-1 (the default) "
        "returns the LATEST 'size' events (tail) — the right choice for 'show me the most "
        "recent reply', and it works even on huge sessions without paginating. offset>=0 "
        "returns events with seq > offset (forward cursor pagination). The response includes "
        "'next_cursor'; to follow a session forward, call once with offset=-1 to get the "
        "latest, then pass next_cursor as the offset on subsequent calls."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "offset": {
                "type": "integer",
                "description": (
                    "Cursor. -1 (default) = latest 'size' events (tail). "
                    ">=0 = only events with seq > offset."
                ),
            },
            "size": {
                "type": "integer",
                "description": "Max events to return (default 100, server caps at 1000).",
            },
            "wait": {
                "type": "integer",
                "description": "Long-poll seconds (default 2). Always pass >= 2.",
            },
        },
        "required": ["session_id"],
    },
}


def _handle_pok_events(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    offset = args.get("offset", -1)
    size = args.get("size", 100)
    wait = args.get("wait", 2)
    try:
        data = _client().get_events(sid, offset=offset, size=size, wait=wait)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_pane
# ---------------------------------------------------------------------------

POK_PANE_SCHEMA = {
    "name": "pok_pane",
    "description": (
        "Read the raw terminal pane content of a CC session (what's visible on screen). "
        "Useful for checking CC's live progress, permission prompts, or error messages."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_pane(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().get_pane(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_interrupt
# ---------------------------------------------------------------------------

POK_INTERRUPT_SCHEMA = {
    "name": "pok_interrupt",
    "description": "Send ESC to interrupt a running CC session (e.g. dismiss a permission prompt).",
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_interrupt(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().interrupt(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_delete
# ---------------------------------------------------------------------------

POK_DELETE_SCHEMA = {
    "name": "pok_delete",
    "description": "Tear down a CC session — sends /exit, force-deletes the zellij session, marks ended.",
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_delete(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().delete_session(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_upload
# ---------------------------------------------------------------------------

POK_UPLOAD_SCHEMA = {
    "name": "pok_upload",
    "description": (
        "Upload a local file to a CC session's .po-k-inbox/ directory so CC can read it. "
        "Provide either file_path (local path to read and encode) or content_base64 directly."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "filename": {"type": "string", "description": "Bare filename (no slashes). Written to <cwd>/.po-k-inbox/<filename>."},
            "file_path": {"type": "string", "description": "Local file path to read and upload."},
            "content_base64": {"type": "string", "description": "Already-encoded base64 content (alternative to file_path)."},
        },
        "required": ["session_id", "filename"],
    },
}


def _handle_pok_upload(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    filename = args.get("filename", "")
    if not sid or not filename:
        return _err("session_id and filename are required")

    content_b64 = args.get("content_base64", "")
    file_path = args.get("file_path", "")

    if not content_b64 and not file_path:
        return _err("provide file_path or content_base64")

    if file_path and not content_b64:
        p = Path(file_path).expanduser()
        if not p.is_file():
            return _err(f"file not found: {file_path}")
        content_b64 = base64.b64encode(p.read_bytes()).decode()

    try:
        data = _client().upload_file(sid, filename, content_b64)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_cost
# ---------------------------------------------------------------------------

POK_COST_SCHEMA = {
    "name": "pok_cost",
    "description": "Get token usage and cost totals for a CC session.",
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_cost(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().get_cost(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_clear
# ---------------------------------------------------------------------------

POK_CLEAR_SCHEMA = {
    "name": "pok_clear",
    "description": (
        "Send /clear to a CC session to reset its context. "
        "Best followed immediately by a pok_prompt with a new task — "
        "/clear alone is unreliable, but /clear + prompt works."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_clear(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().clear(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_capabilities
# ---------------------------------------------------------------------------

POK_CAPABILITIES_SCHEMA = {
    "name": "pok_capabilities",
    "description": "Get the agents, skills, and MCP servers available in a CC session.",
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
        },
        "required": ["session_id"],
    },
}


def _handle_pok_capabilities(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    try:
        data = _client().get_capabilities(sid)
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_permission
# ---------------------------------------------------------------------------

POK_PERMISSION_SCHEMA = {
    "name": "pok_permission",
    "description": (
        "Answer a pending permission request from CC's MCP approval flow. "
        "Only works for po-k's MCP-based permission requests (not native CC TUI prompts). "
        "Use pok_events to find permission_request events with a request_id."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "request_id": {"type": "string", "description": "Permission request ID from the permission_request event."},
            "behavior": {
                "type": "string",
                "enum": ["allow", "deny"],
                "description": "Whether to allow or deny the requested action.",
            },
            "message": {
                "type": "string",
                "description": "Optional message to send back with the decision.",
            },
        },
        "required": ["session_id", "request_id", "behavior"],
    },
}


def _handle_pok_permission(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    req_id = args.get("request_id", "")
    behavior = args.get("behavior", "")
    if not sid or not req_id or not behavior:
        return _err("session_id, request_id, and behavior are required")
    try:
        data = _client().answer_permission(
            sid, req_id, behavior, args.get("message", "")
        )
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_health
# ---------------------------------------------------------------------------

POK_HEALTH_SCHEMA = {
    "name": "pok_health",
    "description": "Check Xpo-k connectivity — returns version and connected po-k count. No auth required.",
    "parameters": {"type": "object", "properties": {}},
}


def _handle_pok_health(args: dict, **_kw) -> str:
    try:
        data = _client().health()
        return _ok(data)
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Tool: pok_profiles
# ---------------------------------------------------------------------------

POK_PROFILES_SCHEMA = {
    "name": "pok_profiles",
    "description": (
        "List, get, create, update, delete, or merge Xpo-k profiles. "
        "Profiles compose CC configuration (CLAUDE.md, agents, skills, MCP servers, settings). "
        "Use action='list' to see available profiles, 'get' to read one, "
        "'create'/'update'/'delete' to manage, 'merge' to preview a composition."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["list", "get", "create", "update", "delete", "merge"],
                "description": "Operation to perform.",
            },
            "name": {
                "type": "string",
                "description": "Profile name (required for get/update/delete).",
            },
            "profile": {
                "type": "object",
                "description": "Profile data (required for create/update). Fields: claude_md, agents, skills, mcp_servers, hooks, settings, tags.",
            },
            "profiles": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Profile names to merge (for action='merge').",
            },
        },
        "required": ["action"],
    },
}


def _handle_pok_profiles(args: dict, **_kw) -> str:
    action = args.get("action", "")
    name = args.get("name", "")
    try:
        c = _client()
        if action == "list":
            return _ok({"profiles": c.list_profiles()})
        elif action == "get":
            if not name:
                return _err("name is required for get")
            return _ok({"profile": c.get_profile(name)})
        elif action == "create":
            profile = args.get("profile", {})
            if not profile:
                return _err("profile data is required for create")
            if name:
                profile["name"] = name
            return _ok(c.create_profile(profile))
        elif action == "update":
            if not name:
                return _err("name is required for update")
            profile = args.get("profile", {})
            if not profile:
                return _err("profile data is required for update")
            return _ok(c.update_profile(name, profile))
        elif action == "delete":
            if not name:
                return _err("name is required for delete")
            return _ok(c.delete_profile(name))
        elif action == "merge":
            profile_names = args.get("profiles", [])
            if not profile_names:
                return _err("profiles list is required for merge")
            return _ok({"merged": c.merge_profiles(profile_names)})
        else:
            return _err(f"unknown action: {action!r}")
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

_TOOLS = [
    (POK_CLIENTS_SCHEMA, _handle_pok_clients),
    (POK_PROJECTS_SCHEMA, _handle_pok_projects),
    (POK_SESSIONS_SCHEMA, _handle_pok_sessions),
    (POK_CREATE_SCHEMA, _handle_pok_create),
    (POK_PROMPT_SCHEMA, _handle_pok_prompt),
    (POK_STATUS_SCHEMA, _handle_pok_status),
    (POK_WAIT_SCHEMA, _handle_pok_wait),
    (POK_EVENTS_SCHEMA, _handle_pok_events),
    (POK_PANE_SCHEMA, _handle_pok_pane),
    (POK_INTERRUPT_SCHEMA, _handle_pok_interrupt),
    (POK_DELETE_SCHEMA, _handle_pok_delete),
    (POK_UPLOAD_SCHEMA, _handle_pok_upload),
    (POK_COST_SCHEMA, _handle_pok_cost),
    (POK_CLEAR_SCHEMA, _handle_pok_clear),
    (POK_CAPABILITIES_SCHEMA, _handle_pok_capabilities),
    (POK_PERMISSION_SCHEMA, _handle_pok_permission),
    (POK_HEALTH_SCHEMA, _handle_pok_health),
    (POK_PROFILES_SCHEMA, _handle_pok_profiles),
]


def register(ctx) -> None:
    """Register all po-k tools with the Hermes plugin context."""
    for schema, handler in _TOOLS:
        ctx.register_tool(
            name=schema["name"],
            toolset=TOOLSET,
            schema=schema,
            handler=handler,
            check_fn=_check,
            is_async=False,
            description=schema.get("description", ""),
        )
    logger.info("po-k plugin: registered %d tools", len(_TOOLS))
