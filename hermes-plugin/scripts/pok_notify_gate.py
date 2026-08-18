#!/usr/bin/env python3
"""Cron wake-gate for po-k notifications — the backup path behind webhook push.

Install as ``~/.hermes/scripts/pok_notify_gate.py`` and attach it to an
approximately hourly Hermes cron job (see README "Background notifications").
Hermes runs this script *before* building the job's prompt: if the last stdout
line is ``{"wakeAgent": false}`` the agent run is skipped entirely — no LLM
turn, no delivery, no tokens. Any other output becomes the turn's context.

So the cost model is: one HTTP request per tick when nothing is pending, and a
scoped agent turn only when Xpo-k is actually holding work — which is the case
when a webhook push failed, the Hermes gateway was down, or the subscription was
created poll-only.

Contract this script keeps:

* **Metadata only.** It prints notification ids, session ids, kinds and seqs —
  never CC prose. The woken turn reads session content itself with
  ``pok_events``, so untrusted model output never rides in on the trigger.
* **It never acks.** Acking is the agent's job after it has handled the work.
* **It never prints the bearer token**, and it fails closed: any error means
  "don't wake the agent", reported on stderr for the operator.

Environment (same variables the pok plugin already uses):

    XPOK_URL           required, e.g. http://xpo-k.host:8080
    XPOK_TOKEN_FILE    path to the bearer token file (preferred)
    XPOK_TOKEN         inline bearer token (fallback)
    POK_SUBSCRIBER     subscriber identity; default hermes-<hostname>
    POK_GATE_LIMIT     max notifications to summarise (default 10)
    POK_GATE_TIMEOUT   HTTP timeout in seconds (default 10)
"""

from __future__ import annotations

import json
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional

DONT_WAKE = '{"wakeAgent": false}'


def subscriber() -> str:
    return os.getenv("POK_SUBSCRIBER", "").strip() or f"hermes-{socket.gethostname()}"


def _token() -> str:
    path = os.getenv("XPOK_TOKEN_FILE", "").strip()
    if path:
        try:
            return open(os.path.expanduser(path), encoding="utf-8").read().strip()
        except OSError:
            pass
    return os.getenv("XPOK_TOKEN", "").strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


def fetch_pending(url: str, token: str, sub: str, limit: int, timeout: int) -> Dict[str, Any]:
    """GET /notifications for this subscriber. Raises on any transport error."""
    query = urllib.parse.urlencode({"subscriber": sub, "limit": limit, "wait": 0})
    req = urllib.request.Request(f"{url.rstrip('/')}/notifications?{query}")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed scheme
        return json.loads(resp.read().decode("utf-8"))


def summarise(rows: List[Dict[str, Any]], sub: str) -> str:
    """Render the wake context — metadata only, never CC output."""
    lines = [
        f"{len(rows)} po-k notification(s) are queued for subscriber {sub!r} and have "
        "NOT been acknowledged. The webhook push either failed or was never "
        "configured, so handle them now.",
        "",
    ]
    for row in rows:
        parts = [f"id={row.get('id')}", f"session={row.get('session_id')}"]
        kind = row.get("kind")
        if kind == "status":
            parts.append(f"status={row.get('status')}")
        else:
            parts.append(f"event={kind}")
        seq = row.get("seq")
        if isinstance(seq, int) and seq > 0:
            parts.append(f"seq={seq}")
        state = row.get("delivery_state")
        if state and state != "none":
            parts.append(f"push={state}")
        lines.append("- " + " ".join(parts))
    lines += [
        "",
        "For each one: read the session with pok_events (start from the seq above, "
        "small size), handle what it means for the user's work, and only then call "
        "pok_notifications(action='ack', ids=[...]). Do not ack anything you did not "
        "handle — it will be offered again on the next run.",
    ]
    return "\n".join(lines)


def run(
    fetch: Optional[Callable[..., Dict[str, Any]]] = None,
    out=None,
    err=None,
) -> int:
    """Print either the wake context or the no-wake gate. Always returns 0.

    ``fetch`` is injectable so the behaviour is testable without a network.
    Exit code stays 0 on purpose: a non-zero exit makes Hermes wake the agent
    with a "script failed" prompt, which would burn a turn on every tick while
    Xpo-k is unreachable.
    """
    out = out or sys.stdout
    err = err or sys.stderr
    url = os.getenv("XPOK_URL", "").strip()
    if not url:
        print("pok_notify_gate: XPOK_URL is not set", file=err)
        print(DONT_WAKE, file=out)
        return 0

    sub = subscriber()
    fetch = fetch or fetch_pending
    try:
        data = fetch(
            url,
            _token(),
            sub,
            _env_int("POK_GATE_LIMIT", 10),
            _env_int("POK_GATE_TIMEOUT", 10),
        )
    except Exception as e:  # transport, auth, JSON — all fail closed
        # Never interpolate the token or response body into this message.
        print(f"pok_notify_gate: poll failed ({type(e).__name__}): {e}", file=err)
        print(DONT_WAKE, file=out)
        return 0

    rows = data.get("notifications") if isinstance(data, dict) else None
    rows = [r for r in rows if isinstance(r, dict) and r.get("id")] if isinstance(rows, list) else []
    if not rows:
        print(DONT_WAKE, file=out)
        return 0

    print(summarise(rows, sub), file=out)
    return 0


if __name__ == "__main__":
    sys.exit(run())
