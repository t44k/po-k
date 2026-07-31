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


def _call(tools, args: dict) -> dict:
    """Invoke the pok_profiles handler and parse its JSON envelope."""
    return json.loads(tools._handle_pok_profiles(args))


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
# Tests: round trip through the Hermes framework (skipped without hermes-agent)
# --------------------------------------------------------------------------- #

def run_framework() -> int:
    """Verify the schema survives Hermes' sanitizer and arg coercion."""
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


def test_framework_round_trip() -> None:
    assert run_framework() == 0


if __name__ == "__main__":
    rc1 = run()
    rc2 = run_framework()
    sys.exit(rc1 | rc2)
