"""Surface queued po-k notifications on the next Hermes turn.

Why this shape
--------------
Xpo-k queues a notification when a subscribed CC session finishes, but something
still has to make it *visible* to Hermes. The plugin boundary offers exactly one
documented, runtime-independent surface for that:

    ctx.register_hook("pre_llm_call", cb)

``pre_llm_call`` fires once per turn, before the tool loop, in
``agent/conversation_loop.py``; a callback returning ``{"context": "..."}`` has
that text appended to the current turn's user message at API-call time
(ephemeral — never persisted to the session DB). Because it lives in the shared
conversation loop it works in the CLI **and** in the gateway, unlike
``ctx.inject_message()``, which needs a CLI reference and returns False under the
gateway.

So a completion that lands while Hermes is doing something else is picked up on
the *next* turn, whatever that turn is about — no ``pok_wait`` in flight, no
background thread injecting into a conversation it doesn't own.

What this deliberately is NOT: it does not originate a turn. Nothing in the
plugin API can. If Hermes is completely idle, the turn that surfaces the
notification is whatever the user (or a Hermes cron job) sends next. See README
"Background notifications" for the one-line cron config that closes that gap.

Safety rules this module holds itself to
----------------------------------------
* Never raise into the hook (``invoke_hook`` logs and continues, but a raise
  would still cost the turn a warning and lose the notification).
* Never ack. Acking is the agent's explicit act via ``pok_notifications``, so a
  turn that dies after injection leaves the notification pending server-side.
* Never block the turn: ``wait=0`` poll, short connect/read timeout, and at most
  one HTTP call per ``POK_NOTIFY_POLL_SECS``.
* Never poll at all unless this process has a reason to believe a subscription
  exists.

Environment
-----------
``POK_NOTIFY_SURFACE=0``      disable the hook entirely (default on)
``POK_NOTIFY_POLL_SECS``      min seconds between polls (default 30)
``POK_NOTIFY_RESURFACE_SECS`` re-mention an unacked notification after this many
                              seconds (default 600; 0 = mention every turn)
``POK_NOTIFY_TIMEOUT``        HTTP timeout for the in-turn poll (default 3s)
``POK_NOTIFY_PROBE_SECS``     how long a "does this subscriber have any
                              subscription?" answer is cached (default 300)
``POK_NOTIFY_LIMIT``          max notifications named per turn (default 5)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Bound on the in-memory "already mentioned" map, so a long-lived process with a
# chatty session can't grow it without limit.
_MAX_SURFACED = 256

_lock = threading.RLock()
# notification id → monotonic timestamp of the last time we mentioned it
_surfaced: Dict[str, float] = {}
_last_poll: float = 0.0
# None = unknown, True/False = cached answer from the last subscription probe
_has_subscription: Optional[bool] = None
_probe_at: float = 0.0


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


def _enabled() -> bool:
    if not os.getenv("XPOK_URL"):
        return False
    return (os.getenv("POK_NOTIFY_SURFACE", "1").strip().lower()
            not in {"0", "false", "no", "off"})


def reset_for_tests() -> None:
    """Clear the module's process-local state (tests only)."""
    global _last_poll, _has_subscription, _probe_at
    with _lock:
        _surfaced.clear()
        _last_poll = 0.0
        _has_subscription = None
        _probe_at = 0.0


def note_subscription_created() -> None:
    """Called after a successful pok_subscribe: start surfacing without waiting
    for the next probe window."""
    global _has_subscription, _probe_at
    with _lock:
        _has_subscription = True
        _probe_at = time.monotonic()


def note_subscription_deleted() -> None:
    """Called after pok_subscriptions(action='delete'): re-probe rather than
    assume the last subscription is gone (others may remain)."""
    global _has_subscription, _probe_at
    with _lock:
        _has_subscription = None
        _probe_at = 0.0


def note_acked(ids: List[str]) -> None:
    """Drop acked ids from the mentioned-map so it doesn't pin memory."""
    with _lock:
        for i in ids:
            _surfaced.pop(str(i), None)


def _trim_surfaced() -> None:
    if len(_surfaced) <= _MAX_SURFACED:
        return
    for stale, _ts in sorted(_surfaced.items(), key=lambda kv: kv[1])[: len(_surfaced) // 2]:
        _surfaced.pop(stale, None)


def _subscriber() -> str:
    from .tools import _subscriber as resolve

    return resolve({})


def _client():
    from .client import get_client

    return get_client()


def _should_poll(now: float) -> bool:
    """Rate-limit + subscription gate. Holds `_lock`'s invariants only for the
    reads; the probe itself happens outside the lock."""
    global _last_poll
    with _lock:
        if now - _last_poll < _env_int("POK_NOTIFY_POLL_SECS", 30):
            return False
        known = _has_subscription
        probe_fresh = now - _probe_at < _env_int("POK_NOTIFY_PROBE_SECS", 300)
    if known is False and probe_fresh:
        return False
    if known is None or not probe_fresh:
        # Cold start (or a stale answer): one cheap lookup tells us whether this
        # subscriber has anything registered — including subscriptions made
        # before this Hermes process started.
        if not _probe_subscriptions():
            return False
    with _lock:
        _last_poll = now
    return True


def _probe_subscriptions() -> bool:
    global _has_subscription, _probe_at
    try:
        data = _client().list_subscriptions(
            subscriber=_subscriber(),
            timeout=_env_int("POK_NOTIFY_TIMEOUT", 3),
        )
        found = bool(data.get("count") or data.get("subscriptions"))
    except Exception as e:  # network hiccup, Xpo-k down, auth — never fatal
        logger.debug("po-k notifier: subscription probe failed: %s", e)
        found = False
    with _lock:
        _has_subscription = found
        _probe_at = time.monotonic()
    return found


def pending_for_turn() -> List[Dict[str, Any]]:
    """Poll for unacked notifications, honouring the rate limit and gates.

    Returns the notifications worth mentioning this turn (possibly empty). Never
    raises, never acks.
    """
    if not _enabled():
        return []
    now = time.monotonic()
    if not _should_poll(now):
        return []
    try:
        data = _client().poll_notifications(
            subscriber=_subscriber(),
            limit=max(_env_int("POK_NOTIFY_LIMIT", 5), 1),
            wait=0,
            timeout=_env_int("POK_NOTIFY_TIMEOUT", 3),
        )
    except Exception as e:
        # A failed poll must not mark anything as surfaced — the notification
        # stays pending server-side and we retry after the rate-limit window.
        logger.debug("po-k notifier: notification poll failed: %s", e)
        return []

    rows = data.get("notifications") or []
    if not isinstance(rows, list):
        return []
    resurface = _env_int("POK_NOTIFY_RESURFACE_SECS", 600)
    fresh: List[Dict[str, Any]] = []
    with _lock:
        for row in rows:
            if not isinstance(row, dict):
                continue
            nid = str(row.get("id") or "")
            if not nid:
                continue
            last = _surfaced.get(nid)
            if last is not None and (resurface <= 0 or now - last < resurface):
                continue
            _surfaced[nid] = now
            fresh.append(row)
        _trim_surfaced()
    return fresh


def render(rows: List[Dict[str, Any]]) -> str:
    """Format notifications as the context block appended to the user message."""
    if not rows:
        return ""
    lines = [
        "<po-k-notifications>",
        f"{len(rows)} queued po-k session notification(s) — a CC session you "
        "subscribed to reached a boundary while you were busy:",
    ]
    for row in rows:
        sid = row.get("session_id", "?")
        kind = row.get("kind", "?")
        status = row.get("status")
        seq = row.get("seq")
        what = f"status={status}" if kind == "status" else f"event={kind}"
        seq_part = f" seq={seq}" if isinstance(seq, int) and seq > 0 else ""
        lines.append(f"- id={row.get('id')} session={sid} {what}{seq_part}")
    lines += [
        "These are NOT acknowledged yet. If relevant to the user's request, read "
        "the session with pok_events (offset=<cursor>) and then call "
        "pok_notifications(action='ack', ids=[...]). If not relevant right now, "
        "ignore them — they stay queued and will be mentioned again later.",
        "</po-k-notifications>",
    ]
    return "\n".join(lines)


def on_pre_llm_call(**_kwargs: Any) -> Optional[Dict[str, str]]:
    """``pre_llm_call`` hook: return queued notifications as turn context.

    Signature is ``**kwargs`` on purpose — Hermes passes session_id,
    user_message, conversation_history, is_first_turn, model, platform and
    sender_id today, and may add more.
    """
    try:
        rows = pending_for_turn()
        if not rows:
            return None
        text = render(rows)
        logger.info("po-k notifier: surfaced %d notification(s) into the turn", len(rows))
        return {"context": text} if text else None
    except Exception as e:  # belt and braces — the hook must never throw
        logger.debug("po-k notifier: pre_llm_call hook failed: %s", e)
        return None
