# po-k HTTP API

po-k drives Claude Code (CC) sessions inside per-project zellij sessions and
exposes them over HTTP. This document is the full endpoint reference.

## Conventions

- **Base URL** — wherever `po-k serve` binds (config `server.bind`, default
  `127.0.0.1:7070`).
- **Auth** — every endpoint except `GET /health` and `GET /help` requires
  `Authorization: Bearer <token>`. The token is the contents of
  `~/.config/po-k/auth.token` (configurable via `auth.bearer_token_file`).
- **Request content type** — POST/PUT bodies are JSON. Endpoints use axum's
  `Json` extractor, which returns **`415 Unsupported Media Type`** if you omit
  `Content-Type: application/json`.
- **Response content type** — JSON unless noted (SSE is `text/event-stream`;
  `/help` defaults to `text/plain`).
- **Sessions vs zellij sessions** — a po-k *session* is one CC instance with
  its own UUID; it lives inside a per-project zellij session named by
  `zellij.session_prefix` + project name.
- **Cursors** — every event has a monotonic per-session integer `seq`. Read
  endpoints take `since=<seq>` and return a `next_cursor` so callers can resume
  without missing events. `seq` never decreases or repeats within a session.

---

## Public

### `GET /health`

Liveness check.

**Response 200:**
```json
{"ok": true, "version": "0.11.0"}
```

### `GET /help`

This document. Returns `text/plain` by default. With
`Accept: application/json` returns:
```json
{"format": "markdown", "version": "0.11.0", "content": "<this markdown>"}
```

---

## Projects

### `GET /projects`

List projects declared in `po-k.yaml`, each annotated with the session IDs
historically known for the project (DB-derived; survives restarts).

**Response 200:**
```json
[{"name": "po-k", "cwd": "/workspace", "session_ids": ["<sid>", ...]}]
```

---

## Session lifecycle

### `POST /sessions`

Spawn a new session for a project. **Refuses with `409 Conflict` if a session
is already running for the project** — one CC per project; DELETE the existing
one first.

**Body:** `{"project": "<name>"}`

**Response 201:**
```json
{
  "session_id": "<uuid>",
  "project": "<name>",
  "cwd": "/workspace",
  "zellij_session": "po-k-<name>",
  "model": "opus",
  "effort": "xhigh",
  "started_at": "2026-05-28T00:00:00Z",
  "pid": null,
  "hooks_path": "/home/.../hooks.json",
  "mcp_path":   "/home/.../mcp.json"
}
```

**Errors:**
- `404 Not Found` — unknown project name.
- `409 Conflict` — `{"error": "...", "session_id": "<existing-sid>"}`.
- `500 Internal Server Error` — spawn or zellij failure (full anyhow chain).

### `GET /sessions`

List currently running sessions from the in-memory registry. Returns `[]`
after a server restart (the registry is not persisted; the DB still has
history — see `/projects`).

### `GET /sessions/{id}`

Same shape as the `POST` response. `404` if unknown.

### `DELETE /sessions/{id}`

Teardown: types `/exit` into CC, then `zellij delete-session --force`
(removes the resurrectable EXITED entry too), drops the in-memory registry
entry, marks `sessions.ended_at`, appends a `cc_exited` event.

**Response 200:** `{"ok": true, "session_id": "<sid>"}`

---

## Sending input

### `POST /sessions/{id}/messages`

Submit a prompt to CC's REPL.

Implementation details that matter for callers:
1. Blocks until CC's `❯` prompt is on screen (`READY_TIMEOUT` = 120s) so
   the input isn't dropped.
2. Captures the **current event cursor** *before* typing (returned as
   `cursor`).
3. Types the text, then sends Enter as a **separate** write (CC's TUI
   treats text+CR in one write as a paste and won't submit it).

**Body:** `{"text": "Reply with PONG"}`

**Response 200:**
```json
{"ok": true, "bytes": 15, "cursor": 7}
```

The returned `cursor` is the max event seq immediately before the message.
Pass it as `since=` to `/wait` and `/messages` for race-free correlation.

### `POST /sessions/{id}/interrupt`

Sends ESC (interrupt the current turn). **No body.**

**Response 200:** `{"ok": true}`

### `POST /sessions/{id}/clear`

Sends `/clear` into CC (resets CC's context). **No body.** Same readiness gate
as `/messages`.

**Response 200:** `{"ok": true}`

### `POST /sessions/{id}/files`

Base64-decodes a file into `<project.cwd>/.po-k-inbox/<filename>` so CC can
read it (point CC at the inbox via prompt or `--add-dir`).

**Body:**
```json
{"filename": "data.txt", "content_base64": "<base64>"}
```

`filename` must be a bare name — no `/`, `\`, or `..`.

**Response 200:** `{"ok": true, "path": "/workspace/.po-k-inbox/data.txt", "bytes": 1234}`

---

## Reading messages (transcript view)

The **transcript view** is the conversational record only — kinds:
`user_prompt`, `assistant_message`, `tool_use`, `tool_result`, `turn_end`.
Lifecycle and `raw_*` events are filtered out at the SQL level so pagination
stays correct.

### `GET /sessions/{id}/messages?since=<seq>&wait=<sec>`

Long-poll. Returns rows with `seq > since` (default `since=0`); if the page
is empty, blocks up to `wait` seconds (default 30, max 60) on the per-session
event-bus then re-queries.

**Response 200:**
```json
{
  "messages": [
    {"seq": 6, "ts": "...", "kind": "user_prompt", "text": "...", "turn_id": "..."},
    {"seq": 11, "ts": "...", "kind": "assistant_message", "text": "PONG",
     "stop_reason": "end_turn", "turn_id": "..."}
  ],
  "next_cursor": 11
}
```

`next_cursor` = `seq` of the last returned row, or `since` if the page is
empty. A wake from the bus may produce an empty page (the new event was a
non-transcript kind); just re-poll with the unchanged `next_cursor`.

### `GET /sessions/{id}/messages/stream?since=<seq>`

Server-Sent Events stream of the transcript. Each frame:

```
event: <kind>
id: <seq>
data: <event JSON, same shape as a long-poll item>
```

Heartbeats every 15s (`: keepalive` comment). The connection lives until the
client disconnects. Resume after a disconnect by reconnecting with
`?since=<last id>`.

---

## Reading all events (raw view)

Same shape and contract as `/messages` and `/messages/stream`, but **all**
event kinds — includes lifecycle (`cc_started`, `stop`, `notification`,
`session_end`, `cc_exited`), permission events, `raw_<type>` projections, and
the transcript kinds.

### `GET /sessions/{id}/events?since=<seq>&wait=<sec>`

Long-poll.

**Response 200:**
```json
{"events": [...], "next_cursor": <int>}
```

### `GET /sessions/{id}/events/stream?since=<seq>`

SSE.

### `GET /sessions/{id}/cost`

Sums `turn_end` events for per-session cost / token totals.

**Response 200:**
```json
{
  "session_id": "...",
  "total_cost_usd": 0.0,
  "input_tokens": 0,
  "output_tokens": 0,
  "cache_creation_input_tokens": 0,
  "cache_read_input_tokens": 0
}
```

---

## Session state

### `GET /sessions/{id}/status`

Current derived CC status.

**Response 200:**
```json
{
  "session_id": "...",
  "status": "idle",
  "cursor": 12,
  "deciding_event": {"kind": "stop", "seq": 10, "ts": "..."},
  "ended_at": null
}
```

`status` values:
- `working` — CC is processing the current turn (`user_prompt` newer than the
  latest `stop`/`turn_end`).
- `awaiting_input` — CC paused for input: an unresolved `permission_request`
  (no later `permission_decision`), or a `notification` newer than the latest
  stop/activity.
- `idle` — CC finished its turn and is ready for input (latest boundary is
  `stop` or `turn_end`, or the session has never received a prompt).
- `ended` — `session_end`/`cc_exited` event present, or `sessions.ended_at` set.

`deciding_event` is the event that justifies the status (may be `null`).

### `GET /sessions/{id}/pane`

Return the visible content of the session's focused zellij pane — a
ground-truth view of CC's state that doesn't depend on the event stream.
Useful for cross-checking `/status`, manual debugging, and tests.

Requires the session to be **currently registered in memory** (i.e. its zellij
+ MCP socket are live); returns **404** for an ended/unknown session even if
its DB row still exists. Don't poll this at high frequency — every call
round-trips the per-session MCP socket.

**Response 200:**
```json
{
  "session_id": "...",
  "zellij_session": "po-k-...",
  "shows_prompt": true,
  "content": "<raw pane content, may include ANSI / box glyphs>"
}
```

`shows_prompt = true` means a line starts with `❯` — the same heuristic
`/wait` uses to know CC is ready for input. When `status == "working"` you'll
typically see `shows_prompt: false` plus an "esc to interrupt" / "✽" working
indicator inside `content`.

### `GET /sessions/{id}/wait?since=<seq>&timeout=<sec>`

**Block until CC is no longer working,** then return the status.

Race-free via `since`: a non-working status only satisfies the wait if its
`deciding_event.seq > since` — so a stale `stop` from the *previous* turn
cannot cause a false return. On timeout returns the current status with
`"timed_out": true` (HTTP 200, **not** an error) — the caller loops.

- `timeout` default 60s, max 600s. CC turns can outlast a single request;
  re-invoke on `timed_out`.
- `Ended` short-circuits the `since` guard.

**Typical orchestrator flow:**
```sh
CUR=$(curl -s -X POST .../sessions/$SID/messages \
        -H 'authorization: bearer …' -H 'content-type: application/json' \
        -d '{"text":"..."}' | jq -r .cursor)
curl -s ".../sessions/$SID/wait?since=$CUR&timeout=180" -H 'authorization: bearer …'
# When reading the final transcript, pass wait=2 (or longer) — see note below.
curl -s ".../sessions/$SID/messages?since=$CUR&wait=2" -H 'authorization: bearer …'
```

**Timing note — read messages with `wait=` after `/wait` returns.** `/wait`
returns the instant the `Stop` hook lands (fast — straight from CC's hook
subprocess). The turn's final `assistant_message` is projected by the JSONL
tailer a beat later (the tailer polls the transcript file). So if you query
`/messages?since=<cursor>&wait=0` *immediately* after `/wait` returns, you may
see the prompt but not yet the reply. Pass `wait=2` (or higher) on the
follow-up `/messages` call to let the tailer catch up, or use the SSE
`/messages/stream` (frames arrive as soon as the tailer flushes each one).

**Response 200:**
```json
{
  "session_id": "...",
  "status": "idle",
  "cursor": 10,
  "deciding_event": {"kind": "stop", "seq": 10, "ts": "..."}
}
```

On timeout an additional `"timed_out": true` field appears.

---

## Permissions

### `POST /sessions/{id}/permission_requests/{req_id}`

Resolve a pending permission request (CC's MCP `approve` tool is blocked
waiting for a decision; `req_id` came in via the `permission_request` event).

**Body:**
```json
{"behavior": "allow", "message": "ok"}
```
or
```json
{"behavior": "deny", "message": "rejected because ..."}
```

**Response 200:** `{"ok": true}`

---

## Internal (you typically don't call these)

### `POST /sessions/{id}/hooks/{event}`

Ingest a CC hook payload. CC's `hooks.json` (generated by po-k) curls these
endpoints; the orchestrator does not call them directly. **Requires
`Content-Type: application/json`** — without it the call 415s and the hook is
silently dropped (the curl still exits 0).

`{event}` ∈ `UserPromptSubmit | Stop | SubagentStop | PostToolUse | Notification | SessionEnd`.

### `POST /sessions/{id}/mcp/approve`

Called by the per-session `po-k mcp` MCP server (which CC launches via
`--mcp-config`) to request a permission decision. Blocks up to
`cc.permission_timeout` for the orchestrator to resolve the matching
`/permission_requests/{req_id}`.

---

## Event kind reference

Every event has `{seq, ts, kind, ...payload}`. Common kinds:

**Lifecycle (po-k):**
- `cc_started` — session spawned.
- `cc_exited` — session killed via DELETE.
- `cc_recovered` — session re-adopted on po-k restart (see *Restart behaviour*).
- `cc_lost` — recovery found this session's zellij was gone (DB row marked ended).

**From CC hooks** (only after `content-type: application/json` is set in
the curl — see internal section):
- `user_prompt` — user submitted a prompt (UserPromptSubmit).
- `stop` — main agent finished a turn (the key WAIT signal).
- `subagent_stop` — a subagent finished. Does NOT idle the main agent.
- `tool_result` — a tool use completed (PostToolUse).
- `notification` — CC sent a notification (often = waiting for input).
- `session_end` — CC exiting.

**From CC's JSONL transcript** (the tailer):
- `user_prompt` — also projected from the transcript (so for the same prompt
  you see two `user_prompt` rows — one from the hook, one from the tailer;
  payloads differ).
- `assistant_message` — text reply with `stop_reason`, `turn_id`.
- `tool_use` — `{id, name, input, turn_id}`.
- `tool_result` — also projected (duplicates the hook).
- `turn_end` — `type=result` line. Common in `--print` mode; rare interactive.
- `raw_<type>` — any other transcript type (`attachment`, `system`,
  `ai-title`, `mode`, ...).

**Permissions:**
- `permission_request` — CC asked for approval.
- `permission_decision` — orchestrator resolved it.

---

## Restart behaviour

po-k's running-session registry lives in memory; the events table and the
per-session `~/.cache/po-k/sessions/<sid>/{hooks.json,mcp.json}` live on disk.
On startup po-k **recovers** automatically:

1. Walk the DB for sessions with `ended_at IS NULL`.
2. For each, check the zellij session is listed *and* its MCP socket actually
   answers (the listing alone includes resurrectable "EXITED" zombies).
3. If alive → re-insert into the in-memory registry and re-attach the JSONL
   tailer; emit a `cc_recovered` lifecycle event.
4. If gone → mark `sessions.ended_at = now()` and emit a `cc_lost` event.

The JSONL tailer resumes from `sessions.last_jsonl_offset` (a per-session
byte position bumped atomically with each tailed event), so no events are
lost or duplicated across the restart.

Because `hooks.json` persists with the bearer token baked in and the server's
`base_url` is fixed in config, CC subprocesses **keep firing hooks at the new
server during the gap and after it returns** — `stop`/`notification`/etc.
events flow through unchanged.

Two extra event kinds you may see in `/events`:
- `cc_recovered` — emitted when recovery re-adopted this session on startup.
- `cc_lost`     — emitted when recovery found this session's zellij was gone.

Neither is status-relevant; `derive_status` ignores them.

## Long-poll & SSE contract

Long-poll (`/messages`, `/events`, `/wait`):
- `since` default `0`; returns rows with `seq > since`.
- `wait` (`/messages`, `/events`) default 30s, max 60s.
- `timeout` (`/wait`) default 60s, max 600s.
- Empty wake is possible (the new event was filtered out by the kind filter,
  or `wait` re-checked status and stayed `working`); re-poll.

SSE (`/messages/stream`, `/events/stream`):
- Frame: `event: <kind>\nid: <seq>\ndata: <json>\n\n`.
- Heartbeat: `: keepalive\n\n` every 15s.
- Resume with `?since=<last id>` after a disconnect.
