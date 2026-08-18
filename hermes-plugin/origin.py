"""Capture where a request came from, so a webhook-woken turn can report back.

A webhook-triggered Hermes turn runs in a fresh, isolated session
(``webhook:<route>:<delivery_id>``) — that isolation is what stops it
interrupting the user's conversation, and it is also why it knows nothing about
the Zulip stream/topic that asked for the work. Nothing in the notification path
can recover that after the fact, so it has to be captured at subscribe time,
from the turn that is *doing* the subscribing.

The only documented plugin API that sees a message's routing metadata is the
``pre_gateway_dispatch`` hook (``gateway/run.py``), which is invoked once per
inbound gateway event with ``event`` (a ``MessageEvent``), ``gateway`` and
``session_store``. ``event.source`` is a ``SessionSource`` carrying platform,
chat_id, chat_name, chat_type, thread_id, parent_chat_id, message_id, user_id
and user_name (``gateway/session.py``). We record a sanitised copy per session
key and return ``None`` so dispatch is completely unaffected.

Deliberate limits:

* **Routing metadata only.** An allow-list of scalar fields; never message text,
  never CC output, never credentials. Xpo-k re-validates and size-caps the same
  allow-list, so a bug here cannot turn origin into a payload channel.
* **CLI turns have no origin.** ``pre_gateway_dispatch`` never fires outside the
  gateway, so ``current()`` returns ``{}`` and the subscription is created
  without origin — the webhook route then falls back to its configured channel.
* **Bounded and non-authoritative.** A small LRU of recent sessions; losing an
  entry costs the fallback route, never correctness.

``POK_FALLBACK_CHAT_ID`` / ``POK_FALLBACK_THREAD_ID`` provide a static origin
for deployments that want reports to land somewhere specific even when the
subscription was created outside a chat (e.g. from a cron turn).
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Keys Xpo-k accepts (crates/xpo-k/src/subs.rs::ORIGIN_KEYS). Anything else is
# dropped here as well, so the two ends cannot drift silently.
ORIGIN_KEYS = (
    "platform",
    "chat_id",
    "chat_name",
    "chat_type",
    "thread_id",
    "parent_chat_id",
    "message_id",
    "user_id",
    "user_name",
    "session_key",
    "hint",
)
# Mirrors ORIGIN_MAX_FIELD on the Xpo-k side.
MAX_FIELD = 300
_MAX_SESSIONS = 64

_lock = threading.RLock()
# session_key → sanitised origin dict, insertion-ordered (acts as the LRU)
_origins: "dict[str, Dict[str, str]]" = {}
_last_key: Optional[str] = None


def reset_for_tests() -> None:
    global _last_key
    with _lock:
        _origins.clear()
        _last_key = None


def _clean(value: Any) -> str:
    """Coerce one field to a safe scalar string, or '' to drop it."""
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    # Control characters would corrupt prompt templates and log lines; Xpo-k
    # rejects them outright, so never send them.
    text = "".join(ch for ch in text if not (ord(ch) < 32 or ord(ch) == 127))
    return text[:MAX_FIELD]


def _from_source(source: Any) -> Dict[str, str]:
    """Project a gateway ``SessionSource`` onto the allow-list."""
    platform = getattr(source, "platform", None)
    platform_name = getattr(platform, "value", None) or platform
    raw = {
        "platform": platform_name,
        "chat_id": getattr(source, "chat_id", None),
        "chat_name": getattr(source, "chat_name", None),
        "chat_type": getattr(source, "chat_type", None),
        "thread_id": getattr(source, "thread_id", None),
        "parent_chat_id": getattr(source, "parent_chat_id", None),
        "message_id": getattr(source, "message_id", None),
        "user_id": getattr(source, "user_id", None) or getattr(source, "user_id_alt", None),
        "user_name": getattr(source, "user_name", None),
    }
    out = {k: _clean(v) for k, v in raw.items()}
    out = {k: v for k, v in out.items() if v}
    if out.get("chat_id"):
        who = out.get("user_name") or out.get("user_id") or "someone"
        where = out["chat_id"] + (f" > {out['thread_id']}" if out.get("thread_id") else "")
        out["hint"] = _clean(f"requested by {who} in {where}")
    return out


def _session_key(gateway: Any, source: Any) -> str:
    """Best-effort stable key for this conversation.

    Prefers the gateway's own session-key builder so the value matches what
    Hermes uses internally; falls back to a composed key.
    """
    try:
        if gateway is not None and hasattr(gateway, "_session_key_for_source"):
            key = gateway._session_key_for_source(source)
            if key:
                return str(key)
    except Exception as e:  # pragma: no cover — defensive
        logger.debug("po-k origin: gateway session key unavailable: %s", e)
    platform = getattr(getattr(source, "platform", None), "value", "unknown")
    return ":".join(
        str(p)
        for p in (platform, getattr(source, "chat_id", ""), getattr(source, "thread_id", "") or "")
    )


def on_pre_gateway_dispatch(**kwargs: Any) -> None:
    """``pre_gateway_dispatch`` hook: remember this turn's origin.

    Always returns ``None`` (= "allow"), so message flow is untouched. Never
    raises: an origin we failed to capture only costs the fallback route.
    """
    global _last_key
    try:
        event = kwargs.get("event")
        source = getattr(event, "source", None)
        if source is None:
            return None
        origin = _from_source(source)
        if not origin.get("chat_id"):
            return None
        key = _session_key(kwargs.get("gateway"), source)
        origin["session_key"] = _clean(key)
        with _lock:
            # Re-insert so the most recent session is last (LRU order).
            _origins.pop(key, None)
            _origins[key] = origin
            while len(_origins) > _MAX_SESSIONS:
                _origins.pop(next(iter(_origins)))
            _last_key = key
    except Exception as e:  # pragma: no cover — defensive
        logger.debug("po-k origin: capture skipped: %s", e)
    return None


def _fallback() -> Dict[str, str]:
    chat = _clean(os.getenv("POK_FALLBACK_CHAT_ID", ""))
    if not chat:
        return {}
    out = {"chat_id": chat, "hint": "configured fallback target"}
    thread = _clean(os.getenv("POK_FALLBACK_THREAD_ID", ""))
    if thread:
        out["thread_id"] = thread
    platform = _clean(os.getenv("POK_FALLBACK_PLATFORM", ""))
    if platform:
        out["platform"] = platform
    return out


def current(session_key: str = "") -> Dict[str, str]:
    """Origin for `session_key`, else the most recent one, else the fallback.

    Returns `{}` when nothing is known (CLI runs), which Xpo-k stores as "no
    origin" and the webhook route resolves to its configured channel.
    """
    with _lock:
        if session_key and session_key in _origins:
            return dict(_origins[session_key])
        if _last_key and _last_key in _origins:
            return dict(_origins[_last_key])
    return _fallback()


def for_lookup(session_key: str = "") -> Dict[str, str]:
    """The subset used to find a workflow by where the user is talking."""
    origin = current(session_key)
    return {
        k: origin[k] for k in ("chat_id", "thread_id") if origin.get(k)
    }


def known_sessions() -> int:
    with _lock:
        return len(_origins)
