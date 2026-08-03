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
from typing import Any, Dict, Optional, Tuple

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


def _notifier_call(fn_name: str, *fn_args) -> None:
    """Nudge the notifier's process-local state. Best effort — the tools must
    work even if the hook module is unavailable."""
    try:
        from . import notifier

        getattr(notifier, fn_name)(*fn_args)
    except Exception as e:  # pragma: no cover — defensive
        logger.debug("po-k: notifier.%s skipped: %s", fn_name, e)


def _subscriber(args: dict) -> str:
    """Resolve the subscriber identity for notification subscriptions.

    Subscriptions are server-side and outlive any single tool call, so they
    need a stable name for this Hermes instance. Explicit `subscriber` wins,
    then `POK_SUBSCRIBER`, then a hostname-derived default — never a random id,
    which would orphan subscriptions across restarts.
    """
    explicit = str(args.get("subscriber") or "").strip()
    if explicit:
        return explicit
    env = os.getenv("POK_SUBSCRIBER", "").strip()
    if env:
        return env
    import socket

    return f"hermes-{socket.gethostname()}"


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
        "Block until a CC session reaches a NEW turn boundary — idle, awaiting_input, "
        "or ended — and return the deciding event.\n\n"
        "Cursor rule: 'since' is compared against the *boundary* seq, not the tail of "
        "the event stream. Pass the 'cursor' returned by pok_prompt, or the "
        "'boundary_cursor' from a previous pok_wait/pok_status. Never pass pok_events' "
        "'next_cursor' (a tail cursor, normally higher than the boundary — the wait "
        "would block until the next turn). When 'since' is omitted this tool resolves "
        "the session's current boundary_cursor first, so it waits for the NEXT "
        "completion instead of returning a stale one.\n\n"
        "Max server-side timeout is 600s; returns timed_out: true on timeout. For long "
        "tasks where you cannot keep a call blocked, use pok_subscribe + pok_notifications."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID."},
            "since": {
                "type": "integer",
                "description": (
                    "Boundary cursor: pok_prompt's 'cursor', or 'boundary_cursor' from a "
                    "previous pok_wait/pok_status. Omit to auto-resolve the current boundary."
                ),
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
    timeout = min(args.get("timeout", 600), 600)
    try:
        c = _client()
        since = args.get("since")
        resolved_from = "caller"
        if since is None:
            # Defaulting to 0 would make the PREVIOUS turn's stop satisfy the
            # wait immediately (stale completion). Baseline at the session's
            # current boundary so only a new one counts.
            status = c.get_status(sid)
            since = status.get("boundary_cursor", status.get("cursor", 0)) or 0
            resolved_from = "status.boundary_cursor"
        data = c.wait(sid, since=int(since), timeout=timeout)
        return _ok({**data, "since_used": int(since), "since_source": resolved_from})
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
                "description": "Max events to return (default 10, server caps at 1000). ⚠️ Each event carries full payload (tool_use inputs, tool_result content, assistant_message text). After /wait returns idle, use size=3–5 — the stop event is always near the tail. size=100 (old default) can dump 50-100K tokens into context, causing provider timeouts.",
            },
            "wait": {
                "type": "integer",
                "description": (
                    "Long-poll seconds (default 2). ⚠️ Only effective for a cursor read "
                    "(offset>=0) or with follow=true — a plain tail (offset=-1) on a "
                    "session that already has events returns immediately."
                ),
            },
            "follow": {
                "type": "boolean",
                "description": (
                    "Pin a tail request to the session's current cursor and long-poll for "
                    "NEW events only. Use when watching for output you haven't seen yet "
                    "without knowing the cursor. Ignored when offset>=0."
                ),
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
    size = args.get("size", 10)
    wait = args.get("wait", 2)
    follow = bool(args.get("follow", False))
    try:
        data = _client().get_events(sid, offset=offset, size=size, wait=wait, follow=follow)
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

def _freeform_map(description: str) -> Dict[str, Any]:
    """Schema fragment for a profile sub-map with arbitrary keys.

    Returns a fresh dict per call so callers never share (and mutate) the
    nested ``properties`` object.
    """
    return {
        "type": "object",
        "description": description,
        "properties": {},
        "additionalProperties": True,
    }


# Field-level schema for the profile object, mirroring pok_proto::Profile
# (crates/pok-proto/src/profile.rs). Declaring the fields explicitly matters:
# a bare {"type": "object"} with no "properties" gets an empty properties dict
# injected downstream (Hermes' schema sanitizer does this for llama.cpp-style
# grammar backends), and a property-less object with no additionalProperties
# constrains the model to emitting a literal {} — i.e. it cannot express any
# profile data at all. The free-form sub-maps keep additionalProperties: true
# so arbitrary agent/skill/server names still pass through.
_PROFILE_PROPERTIES: Dict[str, Any] = {
    "name": {
        "type": "string",
        "description": "Profile name. The top-level `name` argument wins when both are given.",
    },
    "description": {"type": "string", "description": "Human-readable summary of the profile."},
    "version": {"type": "string", "description": "Profile version (defaults to 1.0.0 server-side)."},
    "tags": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Free-form tags for grouping profiles.",
    },
    "claude_md": {"type": "string", "description": "CLAUDE.md content this profile contributes."},
    "agents": _freeform_map("Agent definitions keyed by agent name."),
    "skills": _freeform_map("Skill definitions keyed by skill name."),
    "mcp_servers": _freeform_map("MCP server definitions keyed by server name."),
    "hooks": _freeform_map("CC hook groups keyed by hook event name."),
    "settings": _freeform_map(
        "CC settings.json fragment (model, effort, permission_mode, env, ...)."
    ),
}

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
                "description": (
                    "Profile data (required for create/update). Pass a real JSON object, "
                    "not a JSON-encoded string. Fields: claude_md, agents, skills, "
                    "mcp_servers, hooks, settings, tags, description, version, name."
                ),
                "properties": _PROFILE_PROPERTIES,
                "additionalProperties": True,
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


_PROFILE_FIELD_HINT = (
    "pass a non-empty object with at least one of: "
    "claude_md, agents, skills, mcp_servers, hooks, settings, tags, description"
)


def _coerce_profile(raw: Any) -> Tuple[Optional[Dict[str, Any]], str]:
    """Normalize the ``profile`` argument to a plain dict.

    Returns ``(profile, error)`` — ``error`` is non-empty only when the value
    is present but unusable. A missing/blank profile yields ``(None, "")`` so
    the caller can emit its own action-specific message.

    Models often emit nested object arguments as a JSON-encoded string, and
    not every dispatch path runs Hermes' ``coerce_tool_args`` (plugin-invoked
    tools go straight to ``registry.dispatch``), so a string profile has to be
    decoded here. Without it, create died on ``profile["name"] = name`` with
    "'str' object does not support item assignment" and update forwarded the
    raw string as the HTTP body — Xpo-k then saw a JSON string instead of an
    object and rejected it.

    The returned dict is always a copy: the handler injects ``name`` into it
    and must not mutate the caller's argument dict.
    """
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None, ""
        try:
            raw = json.loads(text)
        except ValueError as e:
            return None, f"profile is not valid JSON: {e}"
    if raw is None:
        return None, ""
    if not isinstance(raw, dict):
        return None, f"profile must be an object, got {type(raw).__name__}"
    return dict(raw), ""


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
            profile, perr = _coerce_profile(args.get("profile"))
            if perr:
                return _err(perr)
            if not profile:
                return _err(f"profile data is required for create — {_PROFILE_FIELD_HINT}")
            if name:
                profile["name"] = name
            pname = profile.get("name")
            if not isinstance(pname, str) or not pname.strip():
                # Xpo-k's POST /profiles requires a string `name`; fail here
                # with a usable message instead of round-tripping a 400.
                return _err(
                    "profile name is required for create — pass name, "
                    "or set a string 'name' inside profile"
                )
            return _ok(c.create_profile(profile))
        elif action == "update":
            if not name:
                return _err("name is required for update")
            profile, perr = _coerce_profile(args.get("profile"))
            if perr:
                return _err(perr)
            if not profile:
                return _err(f"profile data is required for update — {_PROFILE_FIELD_HINT}")
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
# Tool: pok_subscribe / pok_subscriptions / pok_notifications
#
# Background completion handling. A subscription lives in Xpo-k, so a CC turn
# that finishes while no tool call is blocked still queues a notification;
# nothing is lost because a notification stays pending until it is acked.
#
# Deliberately poll-based: this plugin registers plain tools and must not
# assume a background thread may inject into the conversation (ctx-based
# injection is unavailable in gateway mode). See README "Background
# notifications" for the intended gateway loop.
# ---------------------------------------------------------------------------

POK_SUBSCRIBE_SCHEMA = {
    "name": "pok_subscribe",
    "description": (
        "Register interest in a CC session so its completion is queued server-side "
        "even when no pok_wait is blocked. Subscribe BEFORE sending the prompt: the "
        "subscription starts at the session's current event seq, so it can neither "
        "miss that turn's stop nor fire on history. Then handle other work and call "
        "pok_notifications(action='poll') later. Notifications persist across Xpo-k "
        "restarts and po-k reconnects until acked.\n\n"
        "When a webhook target is configured (webhook_url or POK_WEBHOOK_URL), Xpo-k "
        "also pushes each notification to Hermes immediately, which starts a fresh "
        "isolated turn — you do not have to be polling. The queue remains the source "
        "of truth, so a failed push is recovered by the hourly cron fallback."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "description": "Session UUID to watch."},
            "kinds": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Event kinds to notify on. Default: stop, session_end, cc_exited, "
                    "notification, user_question, permission_request."
                ),
            },
            "statuses": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Derived statuses to notify on. Default: idle, awaiting_input, ended.",
            },
            "ttl_secs": {
                "type": "integer",
                "description": "Lifetime in seconds (default 86400, max 604800). Refreshed on each ack.",
            },
            "subscriber": {
                "type": "string",
                "description": "Subscriber identity. Defaults to POK_SUBSCRIBER or hermes-<hostname>.",
            },
            "webhook_url": {
                "type": "string",
                "description": (
                    "Optional Hermes webhook URL for immediate push (e.g. "
                    "http://127.0.0.1:8644/webhooks/pok). Defaults to POK_WEBHOOK_URL. "
                    "Push starts a fresh isolated Hermes turn as soon as the session "
                    "reaches a boundary; the queue still backs it up."
                ),
            },
            "webhook_secret_env": {
                "type": "string",
                "description": (
                    "Name of the env var on the Xpo-k host holding the webhook HMAC "
                    "secret (default POK_WEBHOOK_SECRET). The secret value is never "
                    "sent through this tool."
                ),
            },
        },
        "required": ["session_id"],
    },
}


def _handle_pok_subscribe(args: dict, **_kw) -> str:
    sid = args.get("session_id", "")
    if not sid:
        return _err("session_id is required")
    kinds = args.get("kinds") or None
    statuses = args.get("statuses") or None
    if kinds is not None and not isinstance(kinds, list):
        return _err("kinds must be an array of event-kind strings")
    if statuses is not None and not isinstance(statuses, list):
        return _err("statuses must be an array of status strings")
    # Webhook push is opt-in per subscription. Only a *reference* to the secret
    # travels — Xpo-k reads the value from its own environment at send time.
    url = str(args.get("webhook_url") or os.getenv("POK_WEBHOOK_URL", "")).strip()
    deliver = None
    if url:
        if not url.startswith(("http://", "https://")):
            return _err("webhook_url must be an http(s) URL")
        deliver = {
            "url": url,
            "secret_env": str(
                args.get("webhook_secret_env")
                or os.getenv("POK_WEBHOOK_SECRET_ENV", "POK_WEBHOOK_SECRET")
            ).strip(),
        }
    try:
        data = _client().create_subscription(
            sid,
            subscriber=_subscriber(args),
            kinds=kinds,
            statuses=statuses,
            ttl_secs=args.get("ttl_secs"),
            cursor=args.get("cursor"),
            deliver=deliver,
        )
        # Let the pre_llm_call hook start surfacing immediately instead of
        # waiting for its next subscription probe.
        _notifier_call("note_subscription_created")
        return _ok(data)
    except Exception as e:
        return _err(str(e))


POK_SUBSCRIPTIONS_SCHEMA = {
    "name": "pok_subscriptions",
    "description": (
        "List or cancel session notification subscriptions. "
        "action='list' shows this subscriber's active subscriptions (and their cursors); "
        "action='delete' cancels one and drops its queued notifications."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["list", "delete"],
                "description": "Operation to perform.",
            },
            "subscription_id": {
                "type": "string",
                "description": "Subscription id (required for delete).",
            },
            "session_id": {"type": "string", "description": "Filter the list by session."},
            "all_subscribers": {
                "type": "boolean",
                "description": "List subscriptions from every subscriber, not just this one.",
            },
            "subscriber": {"type": "string", "description": "Override the subscriber identity."},
        },
        "required": ["action"],
    },
}


def _handle_pok_subscriptions(args: dict, **_kw) -> str:
    action = args.get("action", "")
    try:
        c = _client()
        if action == "list":
            subscriber = "" if args.get("all_subscribers") else _subscriber(args)
            data = c.list_subscriptions(
                subscriber=subscriber, sid=args.get("session_id", "")
            )
            return _ok(data)
        elif action == "delete":
            sub_id = args.get("subscription_id", "")
            if not sub_id:
                return _err("subscription_id is required for delete")
            data = c.delete_subscription(sub_id)
            _notifier_call("note_subscription_deleted")
            return _ok(data)
        else:
            return _err(f"unknown action: {action!r}")
    except Exception as e:
        return _err(str(e))


POK_NOTIFICATIONS_SCHEMA = {
    "name": "pok_notifications",
    "description": (
        "Collect queued session notifications (completions, awaiting-input, session end) "
        "for sessions you subscribed to with pok_subscribe.\n\n"
        "action='poll' returns pending notifications, oldest first — reading does NOT "
        "consume them. action='ack' marks them delivered (idempotent) and advances the "
        "subscription cursor. ALWAYS ack what you have acted on, otherwise the same "
        "notification is returned again; conversely nothing is lost if you crash before "
        "acking. Pass wait>0 to long-poll while you have nothing else to do."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["poll", "ack"],
                "description": "Operation to perform.",
            },
            "ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Notification ids to acknowledge (required for ack).",
            },
            "session_id": {"type": "string", "description": "Only notifications for this session."},
            "limit": {"type": "integer", "description": "Max notifications to return (default 20, max 500)."},
            "wait": {
                "type": "integer",
                "description": "Long-poll seconds when nothing is pending (default 0, max 60).",
            },
            "subscriber": {"type": "string", "description": "Override the subscriber identity."},
        },
        "required": ["action"],
    },
}


def _handle_pok_notifications(args: dict, **_kw) -> str:
    action = args.get("action", "")
    try:
        c = _client()
        if action == "poll":
            data = c.poll_notifications(
                subscriber=_subscriber(args),
                sid=args.get("session_id", ""),
                limit=args.get("limit", 20),
                wait=min(args.get("wait", 0) or 0, 60),
            )
            return _ok(data)
        elif action == "ack":
            ids = args.get("ids")
            if isinstance(ids, str):
                ids = [ids]
            if not ids or not isinstance(ids, list):
                return _err("ids (array of notification ids) is required for ack")
            ids = [str(i) for i in ids]
            data = c.ack_notifications(ids)
            _notifier_call("note_acked", ids)
            return _ok(data)
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
    (POK_SUBSCRIBE_SCHEMA, _handle_pok_subscribe),
    (POK_SUBSCRIPTIONS_SCHEMA, _handle_pok_subscriptions),
    (POK_NOTIFICATIONS_SCHEMA, _handle_pok_notifications),
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
            emoji="🕷️",
        )
    logger.info("po-k plugin: registered %d tools", len(_TOOLS))
