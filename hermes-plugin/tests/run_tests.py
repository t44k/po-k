#!/usr/bin/env python3
"""Unit tests for the pok Hermes plugin — no network, no pytest required.

The Hermes plugin loader handles the package's relative imports via
spec_from_file_location with submodule_search_locations. Outside that loader
(e.g. a plain run from the repo root) those imports fail, so we replicate the
loader machinery here in a single self-contained script.

Usage:
    python3 hermes-plugin/tests/run_tests.py

Optionally set PYTHONPATH=/path/to/hermes-agent to also exercise the
schema-sanitizer / argument-coercion round trip (those checks are skipped
when hermes-agent isn't importable).

Also runnable under pytest, which needs importlib import mode because the
plugin directory name ("hermes-plugin") is not a valid package name:
    python3 -m pytest hermes-plugin/tests/run_tests.py --import-mode=importlib
"""

from __future__ import annotations

import importlib.util
import json
import sys
import traceback
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
MODULE_NAME = "hermes_plugins.pok"


def _load_plugin() -> object:
    """Mimic hermes_cli/plugins.py:_load_directory_module()."""
    if "hermes_plugins" not in sys.modules:
        ns = types.ModuleType("hermes_plugins")
        ns.__path__ = []  # type: ignore[attr-defined]
        ns.__package__ = "hermes_plugins"
        sys.modules["hermes_plugins"] = ns

    spec = importlib.util.spec_from_file_location(
        MODULE_NAME, PLUGIN_DIR / "__init__.py",
        submodule_search_locations=[str(PLUGIN_DIR)],
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    module.__package__ = MODULE_NAME
    module.__path__ = [str(PLUGIN_DIR)]  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- #
# Test harness
# --------------------------------------------------------------------------- #

_failures = 0
_passes = 0


def _check(name: str, cond: bool, detail: str = "") -> None:
    global _failures, _passes
    if cond:
        _passes += 1
        print(f"  ✓ {name}")
    else:
        _failures += 1
        print(f"  ✗ {name}  {detail}")


class FakeClient:
    """Stand-in for XpokClient that records calls instead of doing HTTP.

    ``create_profile`` mirrors the Xpo-k contract (POST /profiles rejects a
    body without a string ``name``, and axum would 500 on a non-object body)
    so a regression shows up as a failed check rather than a silent pass.
    """

    def __init__(self) -> None:
        self.calls: list = []

    def create_profile(self, profile):
        self.calls.append(("create", profile))
        if not isinstance(profile, dict):
            raise AssertionError(
                f"HTTP body must be an object, got {type(profile).__name__}"
            )
        if not isinstance(profile.get("name"), str):
            raise AssertionError("profile must have a string `name`")
        return {"name": profile["name"], "version": "1.0.0"}

    def update_profile(self, name, profile):
        self.calls.append(("update", name, profile))
        if not isinstance(profile, dict):
            raise AssertionError(
                f"HTTP body must be an object, got {type(profile).__name__}"
            )
        return {"name": name, "version": "1.0.1"}

    def list_profiles(self):
        self.calls.append(("list",))
        return [{"name": "base"}]

    # -- M15: wait/events cursor discipline + subscriptions --

    def get_status(self, sid):
        self.calls.append(("get_status", sid))
        # Mirrors po-k: `cursor` is the event-stream tail, `boundary_cursor` the
        # deciding turn boundary. They differ whenever anything follows the stop.
        return {"session_id": sid, "status": "idle", "cursor": 30, "boundary_cursor": 20}

    def wait(self, sid, since=0, timeout=600):
        self.calls.append(("wait", sid, since, timeout))
        return {"session_id": sid, "status": "idle", "cursor": 30, "boundary_cursor": 20}

    def get_events(self, sid, offset=-1, size=10, wait=2, follow=False):
        self.calls.append(("get_events", sid, offset, size, wait, follow))
        return {"events": [], "next_cursor": offset if offset >= 0 else 0}

    def create_subscription(self, sid, *, subscriber, kinds=None, statuses=None,
                            ttl_secs=None, cursor=None, deliver=None):
        self.calls.append(("create_subscription", sid, subscriber, kinds, statuses,
                           ttl_secs, cursor, deliver))
        return {"subscription_id": "sub-1", "session_id": sid, "subscriber": subscriber,
                "cursor": 20, "cursor_source": "session",
                "deliver": {"mode": "webhook" if deliver else "poll"}}

    def set_subscription_delivery(self, subscription_id, *, deliver=None, clear=False):
        self.calls.append(("set_subscription_delivery", subscription_id, deliver, clear))
        return {"ok": True, "subscription_id": subscription_id,
                "deliver": {"mode": "poll" if clear else "webhook"}}

    def list_subscriptions(self, *, subscriber="", sid=""):
        self.calls.append(("list_subscriptions", subscriber, sid))
        return {"subscriptions": [{"id": "sub-1", "sid": sid or "s1"}], "count": 1}

    def delete_subscription(self, subscription_id):
        self.calls.append(("delete_subscription", subscription_id))
        return {"ok": True, "subscription_id": subscription_id}

    def poll_notifications(self, *, subscriber="", sid="", limit=20, wait=0):
        self.calls.append(("poll_notifications", subscriber, sid, limit, wait))
        return {"notifications": [{"id": "ntf-1", "kind": "stop", "seq": 21}], "count": 1}

    def ack_notifications(self, ids):
        self.calls.append(("ack_notifications", list(ids)))
        return {"ok": True, "acked": len(ids), "already_acked": 0}


def _call(tools, args: dict) -> dict:
    """Invoke the pok_profiles handler and parse its JSON envelope."""
    return json.loads(tools._handle_pok_profiles(args))


def _invoke(handler, args: dict) -> dict:
    """Invoke any pok tool handler and parse its JSON envelope."""
    return json.loads(handler(args))


# --------------------------------------------------------------------------- #
# Tests: pok_profiles create/update argument handling
# --------------------------------------------------------------------------- #

def run() -> int:
    global _passes, _failures
    _passes = _failures = 0

    print("Loading plugin …")
    try:
        pkg = _load_plugin()
    except Exception:
        traceback.print_exc()
        print("FAIL: plugin failed to import")
        return 1
    print(f"  ✓ plugin imported ({pkg})")

    from hermes_plugins.pok import tools

    fake = FakeClient()
    tools._client = lambda: fake  # type: ignore[assignment]

    # -- valid structured profile ------------------------------------------
    print("\ncreate: valid structured profile")
    fake.calls.clear()
    res = _call(tools, {
        "action": "create",
        "name": "hermes-writer",
        "profile": {
            "claude_md": "# Writer\nBe concise.",
            "tags": ["writing"],
            "settings": {"model": "opus", "effort": "high"},
            "agents": {"editor": {"description": "edits prose", "model": "sonnet"}},
        },
    })
    _check("create succeeds", res.get("success") is True, str(res))
    _check("create returns server payload", res.get("name") == "hermes-writer", str(res))
    _check("exactly one HTTP call", len(fake.calls) == 1, str(fake.calls))
    body = fake.calls[0][1] if fake.calls else None
    _check("body is a dict (not a JSON string)", isinstance(body, dict), repr(body))
    _check("claude_md preserved verbatim",
           isinstance(body, dict) and body.get("claude_md") == "# Writer\nBe concise.",
           repr(body))
    _check("nested settings kept as an object",
           isinstance(body, dict) and body.get("settings") == {"model": "opus", "effort": "high"},
           repr(body))
    _check("nested agents kept as an object",
           isinstance(body, dict)
           and body.get("agents", {}).get("editor", {}).get("model") == "sonnet",
           repr(body))
    _check("tags kept as a list",
           isinstance(body, dict) and body.get("tags") == ["writing"], repr(body))
    _check("no field dropped (5 keys: 4 given + name)",
           isinstance(body, dict) and set(body) == {
               "claude_md", "tags", "settings", "agents", "name"},
           repr(body))

    # -- name injection ----------------------------------------------------
    print("\ncreate: profile name injection")
    fake.calls.clear()
    res = _call(tools, {"action": "create", "name": "from-arg",
                        "profile": {"claude_md": "x"}})
    _check("name argument injected into body",
           res.get("success") is True and fake.calls[0][1]["name"] == "from-arg",
           str((res, fake.calls)))

    fake.calls.clear()
    res = _call(tools, {"action": "create", "name": "arg-wins",
                        "profile": {"name": "in-profile", "claude_md": "x"}})
    _check("name argument overrides profile.name",
           res.get("success") is True and fake.calls[0][1]["name"] == "arg-wins",
           str((res, fake.calls)))

    fake.calls.clear()
    res = _call(tools, {"action": "create",
                        "profile": {"name": "only-in-profile", "claude_md": "x"}})
    _check("profile.name used when name argument omitted",
           res.get("success") is True and fake.calls[0][1]["name"] == "only-in-profile",
           str((res, fake.calls)))

    fake.calls.clear()
    res = _call(tools, {"action": "create", "profile": {"claude_md": "x"}})
    _check("nameless create rejected client-side", res.get("success") is False, str(res))
    _check("nameless error names the fix",
           "name is required" in res.get("error", ""), str(res))
    _check("nameless create makes no HTTP call", fake.calls == [], str(fake.calls))

    fake.calls.clear()
    res = _call(tools, {"action": "create", "name": "   ",
                        "profile": {"claude_md": "x"}})
    _check("whitespace-only name rejected", res.get("success") is False, str(res))
    _check("whitespace-only name makes no HTTP call", fake.calls == [], str(fake.calls))

    fake.calls.clear()
    res = _call(tools, {"action": "create", "profile": {"name": 42, "claude_md": "x"}})
    _check("non-string profile.name rejected", res.get("success") is False, str(res))
    _check("non-string profile.name makes no HTTP call", fake.calls == [], str(fake.calls))

    # -- empty profile rejected client-side --------------------------------
    print("\ncreate/update: empty profile rejected client-side")
    for label, profile in (
        ("{}", {}),
        ("omitted", None),  # sentinel — key removed below
        ("null", None),
        ('"{}"', "{}"),
        ("blank string", "   "),
    ):
        fake.calls.clear()
        args = {"action": "create", "name": "empty-p", "profile": profile}
        if label == "omitted":
            args.pop("profile")
        res = _call(tools, args)
        _check(f"create with profile={label} rejected", res.get("success") is False, str(res))
        _check(f"create with profile={label} mentions required data",
               "profile data is required for create" in res.get("error", ""), str(res))
        _check(f"create with profile={label} makes no HTTP call",
               fake.calls == [], str(fake.calls))

    fake.calls.clear()
    res = _call(tools, {"action": "update", "name": "empty-p", "profile": {}})
    _check("update with empty profile rejected", res.get("success") is False, str(res))
    _check("update error mentions required data",
           "profile data is required for update" in res.get("error", ""), str(res))
    _check("update with empty profile makes no HTTP call", fake.calls == [], str(fake.calls))

    # -- no accidental JSON stringification --------------------------------
    print("\ncreate/update: JSON-string profile is decoded, not forwarded raw")
    fake.calls.clear()
    res = _call(tools, {
        "action": "create",
        "name": "stringified",
        "profile": json.dumps({"claude_md": "# S", "settings": {"model": "opus"}}),
    })
    _check("create accepts JSON-string profile", res.get("success") is True, str(res))
    body = fake.calls[0][1] if fake.calls else None
    _check("decoded body is a dict", isinstance(body, dict), repr(body))
    _check("decoded body keeps nested object",
           isinstance(body, dict) and body.get("settings") == {"model": "opus"}, repr(body))
    _check("decoded body has injected name",
           isinstance(body, dict) and body.get("name") == "stringified", repr(body))

    fake.calls.clear()
    res = _call(tools, {"action": "update", "name": "stringified",
                        "profile": json.dumps({"claude_md": "# S2"})})
    _check("update accepts JSON-string profile", res.get("success") is True, str(res))
    _check("update forwards a dict, not the raw string",
           isinstance(fake.calls[0][2], dict) and fake.calls[0][2]["claude_md"] == "# S2",
           str(fake.calls))

    print("\ncreate: malformed / wrong-typed profile")
    for label, profile in (
        ("truncated JSON", '{"claude_md": "x"'),
        ("plain prose", "just some text"),
    ):
        fake.calls.clear()
        res = _call(tools, {"action": "create", "name": "bad", "profile": profile})
        _check(f"{label} rejected", res.get("success") is False, str(res))
        _check(f"{label} error says invalid JSON",
               "not valid JSON" in res.get("error", ""), str(res))
        _check(f"{label} makes no HTTP call", fake.calls == [], str(fake.calls))

    for label, profile in (
        ("JSON array", '["claude_md"]'),
        ("native list", [{"claude_md": "x"}]),
        ("number", 42),
    ):
        fake.calls.clear()
        res = _call(tools, {"action": "create", "name": "bad", "profile": profile})
        _check(f"{label} rejected", res.get("success") is False, str(res))
        _check(f"{label} error says object expected",
               "must be an object" in res.get("error", ""), str(res))
        _check(f"{label} makes no HTTP call", fake.calls == [], str(fake.calls))

    # -- caller's args are never mutated -----------------------------------
    print("\ncreate: handler does not mutate caller args")
    fake.calls.clear()
    original = {"claude_md": "x"}
    args = {"action": "create", "name": "no-mutate", "profile": original}
    res = _call(tools, args)
    _check("create succeeded", res.get("success") is True, str(res))
    _check("caller's profile dict untouched (no name injected)",
           original == {"claude_md": "x"}, repr(original))
    _check("body still carries the name",
           fake.calls[0][1]["name"] == "no-mutate", str(fake.calls))

    # -- untouched actions still work --------------------------------------
    print("\nother actions unchanged")
    fake.calls.clear()
    res = _call(tools, {"action": "list"})
    _check("list works", res.get("success") is True and res.get("profiles") == [{"name": "base"}],
           str(res))
    res = _call(tools, {"action": "get"})
    _check("get without name rejected",
           res.get("success") is False and "name is required for get" in res.get("error", ""),
           str(res))
    res = _call(tools, {"action": "update", "profile": {"claude_md": "x"}})
    _check("update without name rejected",
           res.get("success") is False and "name is required for update" in res.get("error", ""),
           str(res))
    res = _call(tools, {"action": "frobnicate"})
    _check("unknown action rejected",
           res.get("success") is False and "unknown action" in res.get("error", ""), str(res))

    # -- schema shape ------------------------------------------------------
    print("\npok_profiles schema")
    prof = tools.POK_PROFILES_SCHEMA["parameters"]["properties"]["profile"]
    _check("profile is an object", prof.get("type") == "object")
    _check("profile declares properties (not a bare free-form object)",
           isinstance(prof.get("properties"), dict) and bool(prof["properties"]))
    for field in ("name", "claude_md", "agents", "skills", "mcp_servers",
                  "hooks", "settings", "tags", "description", "version"):
        _check(f"profile.properties has {field}", field in prof["properties"])
    _check("profile allows extra keys (forward-compat)",
           prof.get("additionalProperties") is True)
    _check("claude_md typed as string",
           prof["properties"]["claude_md"].get("type") == "string")
    _check("tags typed as string array",
           prof["properties"]["tags"].get("type") == "array"
           and prof["properties"]["tags"].get("items", {}).get("type") == "string")
    for field in ("agents", "skills", "mcp_servers", "hooks", "settings"):
        _check(f"{field} is a free-form map",
               prof["properties"][field].get("additionalProperties") is True)
    _check("free-form maps don't share a nested properties dict",
           prof["properties"]["agents"]["properties"]
           is not prof["properties"]["skills"]["properties"])

    print(f"\nresults: {_passes} passed, {_failures} failed")
    return 0 if _failures == 0 else 1


# --------------------------------------------------------------------------- #
# Tests: M15 cursor discipline + notification subscriptions
# --------------------------------------------------------------------------- #

def run_notifications() -> int:
    global _passes, _failures
    _passes = _failures = 0

    from hermes_plugins.pok import tools

    fake = FakeClient()
    tools._client = lambda: fake  # type: ignore[assignment]

    print("\npok_wait: never defaults to a stale since=0")
    fake.calls.clear()
    res = _invoke(tools._handle_pok_wait, {"session_id": "s1"})
    waits = [c for c in fake.calls if c[0] == "wait"]
    _check("wait succeeds", res.get("success") is True, str(res))
    _check("resolved the boundary via get_status",
           ("get_status", "s1") in fake.calls, str(fake.calls))
    _check("since = boundary_cursor (20), not 0 and not the tail 30",
           waits and waits[0][2] == 20, str(fake.calls))
    _check("reports which cursor it used",
           res.get("since_used") == 20 and res.get("since_source") == "status.boundary_cursor",
           str(res))

    fake.calls.clear()
    res = _invoke(tools._handle_pok_wait, {"session_id": "s1", "since": 7})
    waits = [c for c in fake.calls if c[0] == "wait"]
    _check("explicit since is passed through", waits and waits[0][2] == 7, str(fake.calls))
    _check("explicit since skips the status lookup",
           not any(c[0] == "get_status" for c in fake.calls), str(fake.calls))
    _check("since_source records the caller", res.get("since_source") == "caller", str(res))

    fake.calls.clear()
    res = _invoke(tools._handle_pok_wait, {"session_id": "s1", "since": 0})
    waits = [c for c in fake.calls if c[0] == "wait"]
    _check("since=0 is honoured when explicitly asked for (compat)",
           waits and waits[0][2] == 0, str(fake.calls))

    res = _invoke(tools._handle_pok_wait, {})
    _check("wait without session_id rejected", res.get("success") is False, str(res))

    _check("wait schema documents the boundary rule",
           "boundary_cursor" in tools.POK_WAIT_SCHEMA["description"])
    _check("wait schema warns against next_cursor",
           "next_cursor" in tools.POK_WAIT_SCHEMA["description"])

    print("\npok_events: follow flag for a real tail long-poll")
    fake.calls.clear()
    _invoke(tools._handle_pok_events, {"session_id": "s1", "follow": True, "wait": 5})
    calls = [c for c in fake.calls if c[0] == "get_events"]
    _check("follow forwarded to the client", calls and calls[0][5] is True, str(fake.calls))
    fake.calls.clear()
    _invoke(tools._handle_pok_events, {"session_id": "s1"})
    calls = [c for c in fake.calls if c[0] == "get_events"]
    _check("default stays offset=-1, follow off (compat)",
           calls and calls[0][2] == -1 and calls[0][5] is False, str(fake.calls))
    _check("follow is in the schema",
           "follow" in tools.POK_EVENTS_SCHEMA["parameters"]["properties"])

    print("\nsubscriber identity")
    import os as _os
    saved = _os.environ.pop("POK_SUBSCRIBER", None)
    try:
        default_id = tools._subscriber({})
        _check("default identity is hostname-derived and stable",
               default_id.startswith("hermes-") and default_id == tools._subscriber({}),
               default_id)
        _os.environ["POK_SUBSCRIBER"] = "ange-prod"
        _check("POK_SUBSCRIBER wins over the default",
               tools._subscriber({}) == "ange-prod")
        _check("explicit argument wins over the env",
               tools._subscriber({"subscriber": "explicit"}) == "explicit")
    finally:
        _os.environ.pop("POK_SUBSCRIBER", None)
        if saved is not None:
            _os.environ["POK_SUBSCRIBER"] = saved

    print("\npok_subscribe")
    fake.calls.clear()
    res = _invoke(tools._handle_pok_subscribe,
                  {"session_id": "s1", "subscriber": "sub-a", "ttl_secs": 60})
    _check("subscribe succeeds", res.get("success") is True, str(res))
    _check("returns the subscription id", res.get("subscription_id") == "sub-1", str(res))
    call = [c for c in fake.calls if c[0] == "create_subscription"][0]
    _check("session + subscriber forwarded", call[1] == "s1" and call[2] == "sub-a", str(call))
    _check("ttl forwarded", call[5] == 60, str(call))
    _check("no kinds/statuses sent when unset (server defaults apply)",
           call[3] is None and call[4] is None, str(call))

    fake.calls.clear()
    res = _invoke(tools._handle_pok_subscribe,
                  {"session_id": "s1", "kinds": ["stop", "user_question"]})
    call = [c for c in fake.calls if c[0] == "create_subscription"][0]
    _check("explicit kinds forwarded", call[3] == ["stop", "user_question"], str(call))

    print("\npok_subscribe: webhook push target (M16)")
    import os as _os2
    _saved = {k: _os2.environ.get(k) for k in ("POK_WEBHOOK_URL", "POK_WEBHOOK_SECRET_ENV")}
    try:
        for k in _saved:
            _os2.environ.pop(k, None)
        fake.calls.clear()
        _invoke(tools._handle_pok_subscribe, {"session_id": "s1"})
        call = [c for c in fake.calls if c[0] == "create_subscription"][0]
        _check("no webhook configured → poll-only subscription", call[7] is None, str(call))

        fake.calls.clear()
        _invoke(tools._handle_pok_subscribe, {
            "session_id": "s1",
            "webhook_url": "http://127.0.0.1:8644/webhooks/pok",
        })
        call = [c for c in fake.calls if c[0] == "create_subscription"][0]
        _check("webhook_url produces a deliver block",
               isinstance(call[7], dict) and call[7]["url"].endswith("/webhooks/pok"), str(call))
        _check("secret is referenced by env-var NAME, never by value",
               call[7].get("secret_env") == "POK_WEBHOOK_SECRET"
               and "secret" not in {k for k in call[7] if k != "secret_env"},
               str(call[7]))

        _os2.environ["POK_WEBHOOK_URL"] = "http://gateway.internal:8644/webhooks/pok"
        _os2.environ["POK_WEBHOOK_SECRET_ENV"] = "CUSTOM_SECRET_VAR"
        fake.calls.clear()
        _invoke(tools._handle_pok_subscribe, {"session_id": "s1"})
        call = [c for c in fake.calls if c[0] == "create_subscription"][0]
        _check("POK_WEBHOOK_URL is the default target",
               call[7]["url"] == "http://gateway.internal:8644/webhooks/pok", str(call))
        _check("POK_WEBHOOK_SECRET_ENV overrides the referenced var name",
               call[7]["secret_env"] == "CUSTOM_SECRET_VAR", str(call))

        fake.calls.clear()
        res = _invoke(tools._handle_pok_subscribe,
                      {"session_id": "s1", "webhook_url": "file:///etc/passwd"})
        _check("non-http webhook_url rejected client-side", res.get("success") is False, str(res))
        _check("rejected target makes no HTTP call", fake.calls == [], str(fake.calls))
    finally:
        for k, v in _saved.items():
            if v is None:
                _os2.environ.pop(k, None)
            else:
                _os2.environ[k] = v

    _check("pok_subscribe documents the push path",
           "webhook" in tools.POK_SUBSCRIBE_SCHEMA["description"].lower())
    _check("webhook_url is in the schema",
           "webhook_url" in tools.POK_SUBSCRIBE_SCHEMA["parameters"]["properties"])

    res = _invoke(tools._handle_pok_subscribe, {})
    _check("subscribe without session_id rejected", res.get("success") is False, str(res))
    res = _invoke(tools._handle_pok_subscribe, {"session_id": "s1", "kinds": "stop"})
    _check("non-array kinds rejected client-side", res.get("success") is False, str(res))
    _check("kinds error is specific", "kinds must be an array" in res.get("error", ""), str(res))

    print("\npok_subscriptions")
    fake.calls.clear()
    res = _invoke(tools._handle_pok_subscriptions, {"action": "list", "subscriber": "sub-a"})
    call = [c for c in fake.calls if c[0] == "list_subscriptions"][0]
    _check("list scoped to this subscriber by default", call[1] == "sub-a", str(call))
    _check("list returns rows", res.get("count") == 1, str(res))
    fake.calls.clear()
    _invoke(tools._handle_pok_subscriptions, {"action": "list", "all_subscribers": True})
    call = [c for c in fake.calls if c[0] == "list_subscriptions"][0]
    _check("all_subscribers drops the filter", call[1] == "", str(call))
    fake.calls.clear()
    res = _invoke(tools._handle_pok_subscriptions,
                  {"action": "delete", "subscription_id": "sub-1"})
    _check("delete forwards the id",
           ("delete_subscription", "sub-1") in fake.calls, str(fake.calls))
    _check("delete succeeds", res.get("success") is True, str(res))
    res = _invoke(tools._handle_pok_subscriptions, {"action": "delete"})
    _check("delete without id rejected", res.get("success") is False, str(res))
    res = _invoke(tools._handle_pok_subscriptions, {"action": "nope"})
    _check("unknown action rejected",
           res.get("success") is False and "unknown action" in res.get("error", ""), str(res))

    print("\npok_notifications")
    fake.calls.clear()
    res = _invoke(tools._handle_pok_notifications,
                  {"action": "poll", "subscriber": "sub-a", "wait": 5})
    call = [c for c in fake.calls if c[0] == "poll_notifications"][0]
    _check("poll forwards subscriber and wait", call[1] == "sub-a" and call[4] == 5, str(call))
    _check("poll returns notifications", res.get("count") == 1, str(res))
    _check("notification carries an id to ack",
           res["notifications"][0]["id"] == "ntf-1", str(res))
    fake.calls.clear()
    _invoke(tools._handle_pok_notifications,
            {"action": "poll", "subscriber": "sub-a", "wait": 999})
    call = [c for c in fake.calls if c[0] == "poll_notifications"][0]
    _check("poll wait clamped to 60", call[4] == 60, str(call))

    fake.calls.clear()
    res = _invoke(tools._handle_pok_notifications, {"action": "ack", "ids": ["ntf-1", "ntf-2"]})
    _check("ack forwards both ids",
           ("ack_notifications", ["ntf-1", "ntf-2"]) in fake.calls, str(fake.calls))
    _check("ack succeeds", res.get("success") is True and res.get("acked") == 2, str(res))
    fake.calls.clear()
    res = _invoke(tools._handle_pok_notifications, {"action": "ack", "ids": "ntf-9"})
    _check("a bare string id is accepted as a single-element list",
           ("ack_notifications", ["ntf-9"]) in fake.calls, str(fake.calls))
    fake.calls.clear()
    res = _invoke(tools._handle_pok_notifications, {"action": "ack"})
    _check("ack without ids rejected", res.get("success") is False, str(res))
    _check("ack makes no HTTP call when rejected", fake.calls == [], str(fake.calls))
    res = _invoke(tools._handle_pok_notifications, {"action": "ack", "ids": []})
    _check("ack with an empty list rejected", res.get("success") is False, str(res))
    res = _invoke(tools._handle_pok_notifications, {"action": "frob"})
    _check("unknown action rejected", res.get("success") is False, str(res))

    print("\nregistration + schemas")
    names = [s["name"] for s, _ in tools._TOOLS]
    for t in ("pok_subscribe", "pok_subscriptions", "pok_notifications"):
        _check(f"{t} registered", t in names)
    _check("no duplicate tool names", len(names) == len(set(names)))
    _check("21 tools registered", len(tools._TOOLS) == 21, str(len(tools._TOOLS)))
    for schema, _h in tools._TOOLS:
        params = schema["parameters"]
        _check(f"{schema['name']} schema is a well-formed object",
               params.get("type") == "object" and isinstance(params.get("properties"), dict))
        for req in params.get("required", []):
            _check(f"{schema['name']}.required[{req}] exists in properties",
                   req in params["properties"])
    _check("pok_notifications documents the ack duty",
           "ack" in tools.POK_NOTIFICATIONS_SCHEMA["description"].lower())
    _check("pok_subscribe tells the agent to subscribe before prompting",
           "BEFORE" in tools.POK_SUBSCRIBE_SCHEMA["description"])

    print(f"\nnotification results: {_passes} passed, {_failures} failed")
    return 0 if _failures == 0 else 1


# --------------------------------------------------------------------------- #
# Tests: pre_llm_call surfacing hook (deterministic — injected clock, no HTTP)
# --------------------------------------------------------------------------- #

class FakeNotifyClient:
    """Client stub for the notifier: scripted poll results, recorded calls."""

    def __init__(self, subs_count: int = 1) -> None:
        self.calls: list = []
        self.subs_count = subs_count
        self.pending: list = []
        self.fail_poll = False
        self.fail_probe = False

    def list_subscriptions(self, *, subscriber="", sid="", timeout=None):
        self.calls.append(("list_subscriptions", subscriber, timeout))
        if self.fail_probe:
            raise RuntimeError("connection refused")
        return {"count": self.subs_count,
                "subscriptions": [{"id": "sub-1"}] * self.subs_count}

    def poll_notifications(self, *, subscriber="", sid="", limit=20, wait=0, timeout=None):
        self.calls.append(("poll", subscriber, limit, wait, timeout))
        if self.fail_poll:
            raise RuntimeError("timeout")
        return {"notifications": list(self.pending), "count": len(self.pending)}

    def ack_notifications(self, ids):
        self.calls.append(("ack", list(ids)))
        return {"ok": True, "acked": len(ids)}


def run_hook() -> int:
    global _passes, _failures
    _passes = _failures = 0

    import os as _os
    from hermes_plugins.pok import notifier

    # Deterministic clock: the module reads time.monotonic() only.
    clock = {"t": 1000.0}
    real_monotonic = notifier.time.monotonic
    notifier.time.monotonic = lambda: clock["t"]  # type: ignore[assignment]

    fake = FakeNotifyClient()
    notifier._client = lambda: fake  # type: ignore[assignment]
    notifier._subscriber = lambda: "hermes-test"  # type: ignore[assignment]

    saved_env = {k: _os.environ.get(k) for k in (
        "XPOK_URL", "POK_NOTIFY_SURFACE", "POK_NOTIFY_POLL_SECS",
        "POK_NOTIFY_RESURFACE_SECS", "POK_NOTIFY_LIMIT", "POK_NOTIFY_TIMEOUT",
        "POK_NOTIFY_PROBE_SECS")}
    _os.environ["XPOK_URL"] = "http://xpok.test:8080"
    _os.environ["POK_NOTIFY_POLL_SECS"] = "30"
    _os.environ["POK_NOTIFY_RESURFACE_SECS"] = "600"
    _os.environ.pop("POK_NOTIFY_SURFACE", None)

    def ntf(nid, kind="stop", seq=7, status=None, sid="s1"):
        return {"id": nid, "session_id": sid, "kind": kind, "seq": seq,
                "status": status, "payload": {}}

    try:
        print("\nhook: gating")
        notifier.reset_for_tests()
        fake.calls.clear()
        saved_url = _os.environ.pop("XPOK_URL")
        _check("no XPOK_URL → hook is a no-op",
               notifier.on_pre_llm_call() is None and fake.calls == [], str(fake.calls))
        _os.environ["XPOK_URL"] = saved_url

        notifier.reset_for_tests()
        fake.calls.clear()
        _os.environ["POK_NOTIFY_SURFACE"] = "0"
        _check("POK_NOTIFY_SURFACE=0 disables it",
               notifier.on_pre_llm_call() is None and fake.calls == [], str(fake.calls))
        _os.environ.pop("POK_NOTIFY_SURFACE")

        notifier.reset_for_tests()
        fake.calls.clear()
        fake.subs_count = 0
        _check("no subscriptions → probes once, never polls",
               notifier.on_pre_llm_call() is None
               and [c[0] for c in fake.calls] == ["list_subscriptions"],
               str(fake.calls))
        clock["t"] += 31
        fake.calls.clear()
        _check("cached negative probe suppresses further work",
               notifier.on_pre_llm_call() is None and fake.calls == [], str(fake.calls))
        fake.subs_count = 1

        print("\nhook: surfacing")
        notifier.reset_for_tests()
        fake.calls.clear()
        fake.pending = [ntf("ntf-1")]
        res = notifier.on_pre_llm_call(session_id="x", user_message="unrelated question",
                                       is_first_turn=False, model="m", platform="cli")
        _check("returns a dict with a context key",
               isinstance(res, dict) and "context" in res, str(res))
        ctx = res["context"] if isinstance(res, dict) else ""
        _check("context is fenced for the model", ctx.startswith("<po-k-notifications>"), ctx[:60])
        _check("names the notification id", "ntf-1" in ctx, ctx)
        _check("names the session", "s1" in ctx, ctx)
        _check("tells the agent to ack", "pok_notifications(action='ack'" in ctx, ctx)
        _check("says they are not acknowledged", "NOT acknowledged" in ctx, ctx)
        _check("never acks while surfacing",
               not any(c[0] == "ack" for c in fake.calls), str(fake.calls))
        poll = [c for c in fake.calls if c[0] == "poll"][0]
        _check("polls with wait=0 (never blocks the turn)", poll[3] == 0, str(poll))
        _check("polls with a short timeout", poll[4] == 3, str(poll))

        print("\nhook: no duplicate nagging, but nothing is lost")
        fake.calls.clear()
        clock["t"] += 31  # past the poll rate limit
        _check("same notification is not repeated within the resurface window",
               notifier.on_pre_llm_call() is None, "repeated too early")
        _check("…and it polled again (the id is filtered locally, not server-side)",
               any(c[0] == "poll" for c in fake.calls), str(fake.calls))
        fake.calls.clear()
        clock["t"] += 601  # past POK_NOTIFY_RESURFACE_SECS
        res2 = notifier.on_pre_llm_call()
        _check("an unacked notification is re-surfaced later",
               isinstance(res2, dict) and "ntf-1" in res2["context"], str(res2))

        print("\nhook: rate limiting")
        fake.calls.clear()
        clock["t"] += 5  # inside the 30s window
        _check("no HTTP inside the rate-limit window",
               notifier.on_pre_llm_call() is None and fake.calls == [], str(fake.calls))

        print("\nhook: failures never consume a notification")
        notifier.reset_for_tests()
        fake.calls.clear()
        fake.pending = [ntf("ntf-2")]
        fake.fail_poll = True
        _check("a failed poll returns no context", notifier.on_pre_llm_call() is None)
        fake.fail_poll = False
        clock["t"] += 31
        res3 = notifier.on_pre_llm_call()
        _check("the notification is surfaced on the next attempt (not marked delivered)",
               isinstance(res3, dict) and "ntf-2" in res3["context"], str(res3))

        notifier.reset_for_tests()
        fake.calls.clear()
        fake.fail_probe = True
        _check("a failed probe is not fatal", notifier.on_pre_llm_call() is None)
        fake.fail_probe = False

        print("\nhook: ack clears local state")
        notifier.reset_for_tests()
        notifier.note_subscription_created()
        fake.pending = [ntf("ntf-3")]
        res4 = notifier.on_pre_llm_call()
        _check("surfaced once", isinstance(res4, dict) and "ntf-3" in res4["context"])
        notifier.note_acked(["ntf-3"])
        _check("acked ids are dropped from the mentioned-map",
               "ntf-3" not in notifier._surfaced, str(notifier._surfaced))

        print("\nhook: shapes and robustness")
        notifier.reset_for_tests()
        notifier.note_subscription_created()
        fake.calls.clear()
        _check("note_subscription_created skips the probe",
               not any(c[0] == "list_subscriptions" for c in fake.calls), str(fake.calls))
        fake.pending = [ntf("ntf-s", kind="status", seq=-1, status="idle")]
        res5 = notifier.on_pre_llm_call()
        _check("status notifications render as status=…",
               isinstance(res5, dict) and "status=idle" in res5["context"], str(res5))
        _check("no bogus seq for status rows",
               isinstance(res5, dict) and "seq=-1" not in res5["context"], str(res5))
        notifier.reset_for_tests()
        notifier.note_subscription_created()
        fake.pending = [{"garbage": True}, ntf("ntf-4")]
        res6 = notifier.on_pre_llm_call()
        _check("rows without an id are skipped, valid ones still surface",
               isinstance(res6, dict) and "ntf-4" in res6["context"], str(res6))
        notifier.reset_for_tests()
        notifier.note_subscription_created()
        fake.pending = []
        _check("nothing pending → no context", notifier.on_pre_llm_call() is None)
        _check("render([]) is empty", notifier.render([]) == "")

        print("\nhook: bounded memory")
        notifier.reset_for_tests()
        notifier.note_subscription_created()
        fake.pending = [ntf(f"ntf-b{i}") for i in range(notifier._MAX_SURFACED + 50)]
        _os.environ["POK_NOTIFY_LIMIT"] = str(notifier._MAX_SURFACED + 50)
        notifier.on_pre_llm_call()
        _check("mentioned-map stays bounded",
               len(notifier._surfaced) <= notifier._MAX_SURFACED,
               str(len(notifier._surfaced)))
        _os.environ.pop("POK_NOTIFY_LIMIT")

        print("\nregistration")
        registered: list = []

        class _Ctx:
            def register_tool(self, **kw):
                pass

            def register_hook(self, name, cb):
                registered.append((name, cb))

        from hermes_plugins import pok as pkg
        pkg.register(_Ctx())
        _check("plugin registers exactly one hook", len(registered) == 1, str(registered))
        _check("…and it is pre_llm_call", registered and registered[0][0] == "pre_llm_call",
               str(registered))
        _check("…bound to the notifier callback",
               registered and registered[0][1] is notifier.on_pre_llm_call)

        # A Hermes build without register_hook must still load the tools.
        class _OldCtx:
            def register_tool(self, **kw):
                pass

        try:
            pkg.register(_OldCtx())
            _check("plugin still loads when register_hook is unavailable", True)
        except Exception as e:
            _check("plugin still loads when register_hook is unavailable", False, str(e))
    finally:
        notifier.time.monotonic = real_monotonic  # type: ignore[assignment]
        notifier.reset_for_tests()
        for k, v in saved_env.items():
            if v is None:
                _os.environ.pop(k, None)
            else:
                _os.environ[k] = v

    print(f"\nhook results: {_passes} passed, {_failures} failed")
    return 0 if _failures == 0 else 1


# --------------------------------------------------------------------------- #
# Tests: round trip through the Hermes framework (skipped without hermes-agent)
# --------------------------------------------------------------------------- #

def run_gate() -> int:
    """Cron wake-gate script: deterministic, injected fetch, no network."""
    global _passes, _failures
    _passes = _failures = 0
    print("\ncron wake-gate script")

    import importlib.util as _ilu
    import io
    import os as _os

    gate_path = PLUGIN_DIR / "scripts" / "pok_notify_gate.py"
    _check("script ships with the plugin", gate_path.is_file(), str(gate_path))
    spec = _ilu.spec_from_file_location("pok_notify_gate", gate_path)
    gate = _ilu.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(gate)  # type: ignore[union-attr]

    def call(rows=None, raiser=None, url="http://xpok.test:8080"):
        """Run the gate with an injected fetch; returns (stdout, stderr, rc)."""
        out, err = io.StringIO(), io.StringIO()
        saved = _os.environ.get("XPOK_URL")
        if url is None:
            _os.environ.pop("XPOK_URL", None)
        else:
            _os.environ["XPOK_URL"] = url

        def fake_fetch(u, token, sub, limit, timeout):
            calls.append((u, token, sub, limit, timeout))
            if raiser:
                raise raiser
            return {"notifications": rows or [], "count": len(rows or [])}

        calls: list = []
        try:
            rc = gate.run(fetch=fake_fetch, out=out, err=err)
        finally:
            if saved is None:
                _os.environ.pop("XPOK_URL", None)
            else:
                _os.environ["XPOK_URL"] = saved
        return out.getvalue(), err.getvalue(), rc, calls

    def ntf(nid, kind="stop", seq=7, status=None, state="failed"):
        return {"id": nid, "session_id": "s1", "kind": kind, "seq": seq,
                "status": status, "delivery_state": state,
                "payload": {"event": {"payload": {"last_assistant_message": "CC PROSE"}}}}

    _saved_sub = _os.environ.get("POK_SUBSCRIBER")
    _os.environ["POK_SUBSCRIBER"] = "hermes-gate-test"
    try:
        print("\n  empty queue → zero-token tick")
        out, err, rc, calls = call(rows=[])
        _check("prints the no-wake gate as the last line",
               out.strip().splitlines()[-1] == '{"wakeAgent": false}', repr(out))
        _check("gate JSON parses to wakeAgent False",
               json.loads(out.strip().splitlines()[-1]) == {"wakeAgent": False}, repr(out))
        _check("exit code 0 (a non-zero exit would wake the agent)", rc == 0)
        _check("polled with wait=0 and the subscriber",
               calls and calls[0][2] == "hermes-gate-test", str(calls))

        print("\n  pending → wake with metadata only")
        out, err, rc, _ = call(rows=[ntf("ntf-1"), ntf("ntf-2", kind="status", seq=-1, status="idle")])
        _check("does not print the no-wake gate",
               "wakeAgent" not in out, repr(out))
        _check("names both ids", "ntf-1" in out and "ntf-2" in out, out)
        _check("names the session", "session=s1" in out, out)
        _check("renders status rows as status=", "status=idle" in out, out)
        _check("omits a bogus seq for status rows", "seq=-1" not in out, out)
        _check("surfaces the push state for triage", "push=failed" in out, out)
        _check("NO CC prose in the wake context", "CC PROSE" not in out, out)
        _check("no payload blob", "last_assistant_message" not in out, out)
        _check("instructs pok_events", "pok_events" in out, out)
        _check("instructs ack only after handling",
               "pok_notifications(action='ack'" in out and "did not" in out, out)
        _check("tells the agent these are unacknowledged", "NOT been acknowledged" in out, out)

        print("\n  failures fail closed")
        out, err, rc, _ = call(raiser=RuntimeError("connection refused"))
        _check("poll failure → no wake",
               out.strip().splitlines()[-1] == '{"wakeAgent": false}', repr(out))
        _check("poll failure still exits 0", rc == 0)
        _check("diagnostic goes to stderr", "poll failed" in err, err)

        out, err, rc, calls = call(url=None)
        _check("missing XPOK_URL → no wake, no fetch",
               out.strip().splitlines()[-1] == '{"wakeAgent": false}' and calls == [], repr(out))
        _check("missing XPOK_URL is reported on stderr", "XPOK_URL" in err, err)

        print("\n  malformed responses")
        for label, rows in (("None rows", None), ("rows without ids", [{"kind": "stop"}])):
            out, _e, _rc, _c = call(rows=rows)
            _check(f"{label} → no wake",
                   out.strip().splitlines()[-1] == '{"wakeAgent": false}', repr(out))

        print("\n  token handling")
        _saved_tok = _os.environ.get("XPOK_TOKEN")
        _os.environ["XPOK_TOKEN"] = "super-secret-token"
        try:
            out, err, _rc, calls = call(rows=[ntf("ntf-3")])
            _check("token is passed to the fetcher",
                   calls and calls[0][1] == "super-secret-token", "not forwarded")
            _check("token never appears on stdout", "super-secret-token" not in out)
            _check("token never appears on stderr", "super-secret-token" not in err)
            out2, err2, _rc2, _c2 = call(raiser=RuntimeError("boom super-secret-token"))
            _check("an error message carrying the token is still not echoed verbatim to stdout",
                   "super-secret-token" not in out2, out2)
        finally:
            if _saved_tok is None:
                _os.environ.pop("XPOK_TOKEN", None)
            else:
                _os.environ["XPOK_TOKEN"] = _saved_tok

        print("\n  subscriber default")
        _os.environ.pop("POK_SUBSCRIBER")
        _check("defaults to hermes-<hostname>",
               gate.subscriber().startswith("hermes-") and len(gate.subscriber()) > 7,
               gate.subscriber())
        _check("matches the plugin's identity rule",
               gate.subscriber() == _plugin_subscriber(), gate.subscriber())
    finally:
        if _saved_sub is None:
            _os.environ.pop("POK_SUBSCRIBER", None)
        else:
            _os.environ["POK_SUBSCRIBER"] = _saved_sub

    print(f"\ngate results: {_passes} passed, {_failures} failed")
    return 0 if _failures == 0 else 1


def _plugin_subscriber() -> str:
    from hermes_plugins.pok import tools

    return tools._subscriber({})


def pt_notifier_stub():
    """The plugin's real hook callback with a scripted client behind it, so the
    framework round trip exercises our code, not a lambda."""
    from hermes_plugins.pok import notifier

    fake = FakeNotifyClient()
    fake.pending = [{"id": "ntf-fw", "session_id": "s1", "kind": "stop", "seq": 3}]
    notifier.reset_for_tests()
    notifier._client = lambda: fake  # type: ignore[assignment]
    notifier._subscriber = lambda: "hermes-fw"  # type: ignore[assignment]
    notifier.note_subscription_created()
    import os as _os

    _os.environ["XPOK_URL"] = _os.environ.get("XPOK_URL") or "http://xpok.test:8080"
    return notifier.on_pre_llm_call


def run_framework() -> int:
    """Verify the schema survives Hermes' sanitizer and arg coercion, and that
    the surfacing hook plugs into the real plugin manager."""
    global _passes, _failures
    _passes = _failures = 0
    print("\nhermes-agent round trip")

    try:
        from tools.schema_sanitizer import sanitize_tool_schemas
    except Exception as e:
        print(f"  – skipped (hermes-agent not importable: {e})")
        return 0

    from hermes_plugins.pok import tools as pt

    sanitized = sanitize_tool_schemas([
        {"type": "function", "function": pt.POK_PROFILES_SCHEMA},
    ])
    prof = sanitized[0]["function"]["parameters"]["properties"]["profile"]
    _check("sanitizer keeps profile properties", bool(prof.get("properties")),
           json.dumps(prof)[:200])
    _check("sanitizer keeps claude_md", "claude_md" in prof.get("properties", {}))
    _check("sanitizer keeps additionalProperties",
           prof.get("additionalProperties") is True)
    _check("sanitizer keeps required=[action]",
           sanitized[0]["function"]["parameters"].get("required") == ["action"])

    # The surfacing hook must be a REAL Hermes hook, not an invented name, and
    # invoke_hook must actually collect our context dict.
    try:
        from hermes_cli.plugins import VALID_HOOKS, get_plugin_manager

        _check("pre_llm_call is a documented Hermes hook", "pre_llm_call" in VALID_HOOKS)
        mgr = get_plugin_manager()
        mgr._hooks.setdefault("pre_llm_call", [])
        before = list(mgr._hooks["pre_llm_call"])
        mgr._hooks["pre_llm_call"].append(pt_notifier_stub())
        try:
            results = mgr.invoke_hook(
                "pre_llm_call",
                session_id="s", user_message="unrelated", conversation_history=[],
                is_first_turn=False, model="m", platform="cli", sender_id="",
            )
            ctxs = [r["context"] for r in results
                    if isinstance(r, dict) and r.get("context")]
            _check("invoke_hook collects our {'context': ...} return",
                   any("po-k-notifications" in c for c in ctxs), str(results))
        finally:
            mgr._hooks["pre_llm_call"] = before
        # And a raising callback is swallowed by the manager (our hook also
        # guards internally, but confirm the contract we rely on).
        mgr._hooks["pre_llm_call"].append(lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        try:
            mgr.invoke_hook("pre_llm_call", session_id="s")
            _check("a raising hook callback cannot break the turn", True)
        except Exception as e:
            _check("a raising hook callback cannot break the turn", False, str(e))
        finally:
            mgr._hooks["pre_llm_call"] = before
    except Exception as e:
        _check("hook contract verified against hermes-agent", False, str(e))

    try:
        from model_tools import coerce_tool_args
        from tools.registry import registry
    except Exception as e:
        print(f"  – arg-coercion checks skipped ({e})")
        print(f"\nframework results: {_passes} passed, {_failures} failed")
        return 0 if _failures == 0 else 1

    class _Ctx:
        def register_tool(self, **kw):
            registry.register(
                name=kw["name"], toolset=kw["toolset"], schema=kw["schema"],
                handler=kw["handler"], check_fn=kw.get("check_fn"),
                is_async=kw.get("is_async", False),
                description=kw.get("description", ""), emoji=kw.get("emoji", ""),
            )

    pt.register(_Ctx())
    coerced = coerce_tool_args("pok_profiles", {
        "action": "create", "name": "rt",
        "profile": '{"claude_md": "# RT"}',
    })
    _check("registry knows pok_profiles", registry.get_schema("pok_profiles") is not None)
    _check("framework coerces a JSON-string profile to a dict",
           isinstance(coerced.get("profile"), dict), repr(coerced.get("profile")))

    print(f"\nframework results: {_passes} passed, {_failures} failed")
    return 0 if _failures == 0 else 1


# Thin wrappers so `pytest hermes-plugin/tests` also picks these up; the
# script above stays the primary entry point (matches the hermes-zulip plugin).
def test_pok_profiles() -> None:
    assert run() == 0


def test_notifications() -> None:
    assert run_notifications() == 0


def test_surfacing_hook() -> None:
    assert run_hook() == 0


def test_cron_wake_gate() -> None:
    assert run_gate() == 0


def test_framework_round_trip() -> None:
    assert run_framework() == 0


if __name__ == "__main__":
    rc1 = run()
    rc2 = run_notifications()
    rc3 = run_hook()
    rc4 = run_gate()
    rc5 = run_framework()
    sys.exit(rc1 | rc2 | rc3 | rc4 | rc5)
