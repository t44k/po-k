# po-k HTTP API

po-k drives Claude Code (CC) sessions inside zellij and exposes them over HTTP.
Every dev box runs one `po-k serve`. The po-k next to your orchestrator is also
a **hub**: it can connect to other boxes, proxy their session API, and call a
webhook when a remote turn finishes. `GET /docs` is the machine-readable twin of
this page (routes, JSON Schemas, defaults).

## Conventions

- **Base URL** — `http://<box>:13658` (config `server.bind`, default `0.0.0.0:13658`).
- **Auth** — every route except `GET /health`, `GET /help`, `GET /docs` needs
  `Authorization: Bearer <token>`. One token for the whole fleet
  (`~/.config/po-k/auth.token`); the hub uses the same token to call other boxes.
- **Bodies** — JSON. `Content-Type` is not required. Bad or unknown fields give a
  JSON `400 {"error": "..."}` naming the field.
- **Version handshake** — every po-k → po-k request (hub → box, `po-k mcp` →
  local serve) carries `x-pok-version`. A different build is refused with
  `409 {"error": "version mismatch: ...", "server_version", "client_version"}`
  before anything else runs; every response carries the server's version in
  the same header. `POST /hosts` also compares the remote `/health` version.
- **Sessions vs zellij sessions** — a po-k *session* is one CC instance with a
  UUID; it runs inside a zellij session named `po-k-<name>`. Only one live
  session per name (`409` otherwise).
- **Cursors** — every event has a per-session monotonic integer `seq`. Three
  cursors travel through this API (see *Cursors*). Getting them wrong is the
  classic reason "the orchestrator never noticed the turn finished".

---

## Public

### `GET /health`
`{"ok": true, "version": "0.12.0", "sessions": 2, "hosts": 3, "watches": 1}`

### `GET /help`
This document (`text/plain`). With `Accept: application/json`:
`{"format": "markdown", "version": "...", "content": "<markdown>"}`.

### `GET /docs`
JSON: every route with its query parameters, the JSON Schema of every request
body (`schemas.create_session` is what you want before `POST /sessions`), the
defaults po-k fills in, status/event vocabularies, cursor rules, the webhook
contract and a quickstart.

---

## Create a session

### `POST /sessions`

Start CC in a directory on **this** box. Nothing is preconfigured: the request
carries everything.

```json
{
  "cwd": "/workspace",                          // required, absolute; created if missing
  "name": "api",                                // default: slug of basename(cwd)
  "model": "fable",                             // default fable
  "effort": "xhigh",                            // default xhigh
  "permission_mode": "bypassPermissions",       // default; see /docs defaults.permission_modes
  "plugins": ["/zirzen/base/plugins/sapi", "https://host/x.zip"],   // --plugin-dir / --plugin-url
  "mcp_servers": {                              // extra MCP servers for CC (.mcp.json entry shape)
    "linear": {"command": "node", "args": ["/opt/linear-mcp/index.js"], "env": {"LINEAR_TOKEN": "${LINEAR_TOKEN}"}},
    "docs":   {"type": "http", "url": "https://mcp.example.com/mcp", "headers": {"Authorization": "Bearer ${DOCS_TOKEN}"}}
  },
  "agent": "lead-reviewer",                     // --agent, must exist in a loaded plugin
  "add_dirs": ["/other/repo"],                  // extra --add-dir; cwd is always included
  "system_prompt": "You are reviewing PR 42."   // --append-system-prompt-file
}
```

Rules: `plugins` are absolute paths that exist on the box, or `http(s)` URLs.
`mcp_servers` entries need a `command` (stdio) or a `url` (`type: http|sse`);
the name `po-k` is reserved. `bare: true` is rejected (it disables the hooks
status depends on). Unknown fields → 400.

**Response 201** (the session view, also returned by `GET /sessions[/{id}]`):
```json
{"session_id": "<uuid>", "name": "api", "cwd": "/workspace", "zellij_session": "po-k-api",
 "model": "fable", "effort": "xhigh", "permission_mode": "bypassPermissions", "agent": null,
 "plugins": ["/zirzen/base/plugins/sapi"], "mcp_servers": ["linear", "docs"],
 "started_at": "2026-09-05T12:00:00Z", "pid": null,
 "hooks_path": "~/.cache/po-k/sessions/<sid>/hooks.json", "mcp_path": "~/.cache/po-k/sessions/<sid>/mcp.json"}
```
Errors: `400` bad field, `409 {"error", "session_id"}` a session with that name
is running (delete it or pass another `name`), `500` zellij/spawn failure.

po-k always injects its own permission MCP server (`po-k cc-mcp`) and lifecycle
hooks; a request cannot override them. Before launching, po-k marks `cwd` as
trusted in `~/.claude.json` so CC's first-run "trust this folder?" dialog never
blocks a session in a new directory. Plugins provide agents, skills, CLAUDE.md
and their own `.mcp.json`; `GET /sessions/{id}/capabilities` shows what loaded.

### `GET /sessions` · `GET /sessions/{id}` · `DELETE /sessions/{id}`
List running sessions / one session / tear down (`/exit` into CC, kill the
zellij session, mark ended, append `cc_exited`). `DELETE` → `{"ok": true, "session_id"}`.

---

## Sending input

### `POST /sessions/{id}/messages`
`{"text": "Reply with PONG"}` → `{"ok": true, "bytes": 15, "cursor": 7}`.
Waits for CC's `❯` prompt (up to 120 s), captures the event cursor **before**
typing, types the text, then Enter. The returned `cursor` is the **boundary
cursor** to arm `/wait` with.

### `POST /sessions/{id}/interrupt` — send ESC. `{"ok": true}`
### `POST /sessions/{id}/clear` — send `/clear`. `{"ok": true}`
### `POST /sessions/{id}/files`
`{"filename": "data.txt", "content_base64": "..."}` → written to
`<cwd>/.po-k-inbox/data.txt`. `filename` must be a bare name.

---

## Reading

### `GET /sessions/{id}/messages?offset=&size=[&wait=&follow=]`
Transcript view only (`user_prompt`, `assistant_message`, `tool_use`,
`tool_result`, `turn_end`). `offset` and `size` are **required**:
`offset >= 0` returns rows with `seq > offset`; `offset=-1` returns the latest
`size` rows immediately. `wait` (default 30, max 60) long-polls an empty page.
`follow=1` with `offset=-1` pins to the current cursor and long-polls for NEW
rows only. Response `{"messages": [...], "next_cursor": <tail seq>}`.

### `GET /sessions/{id}/events?offset=&size=[&wait=&follow=]`
Same contract, all event kinds. `{"events": [...], "next_cursor"}`.

### `GET /sessions/{id}/messages/stream` · `GET /sessions/{id}/events/stream`
Server-Sent Events, `?since=<seq>` to resume. Frame:
`event: <kind>\nid: <seq>\ndata: <json>\n\n`; `: keepalive` every 15 s.

### `GET /sessions/{id}/cost`
`{"session_id", "total_cost_usd", "input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"}`
summed from `turn_end` events.

---

## Session state

### `GET /sessions/{id}/status`
```json
{"session_id": "...", "status": "idle", "cursor": 12, "boundary_cursor": 10,
 "deciding_event": {"kind": "stop", "seq": 10, "ts": "..."}, "ended_at": null}
```
`status`: `working` (a `user_prompt` is newer than the last `stop`),
`awaiting_input` (unresolved `permission_request`, or a `notification` /
`user_question` newer than the last stop), `idle` (last boundary is a `stop`, or
no prompt yet), `ended`. `deciding_event.payload` is included for
`user_question` and `permission_request` so you can see what CC is asking.

### `GET /sessions/{id}/wait?since=<boundary>&timeout=<sec>`
Block until CC reaches a turn boundary **newer than `since`** (idle,
awaiting_input, or ended), then return the status shape above. On timeout
(default 60 s, max 600 s) returns `200` with `"timed_out": true` — loop.
`since` must be a **boundary** cursor (`cursor` from `POST /messages`, or
`boundary_cursor`); `since=0` means any past boundary satisfies immediately.

### `GET /sessions/{id}/pane`
`{"session_id", "zellij_session", "shows_prompt", "content"}` — the raw pane,
ground truth when the event stream looks wrong. 404 for ended sessions.

### `GET /sessions/{id}/capabilities`
For each plugin the session loaded: agents, skills, MCP servers, CLAUDE.md
summary; the request's MCP server names; effective model/effort/permission
mode; warnings (e.g. a plugin defining an MCP server named `po-k`).

---

## Cursors

| Cursor | Where | Meaning | Use |
|---|---|---|---|
| **tail** | `cursor` on `/status` `/wait`; `next_cursor` on `/events` | highest seq stored | page forward: `/events?offset=<tail>` |
| **boundary** | `boundary_cursor` on `/status` `/wait`; `cursor` from `POST /messages` | seq of the deciding stop/notification | `/wait?since=<boundary>` |
| **webhook** | `boundary_cursor` in every webhook body | same as boundary | the next `/wait?since=` |

Never re-arm `/wait` with `next_cursor`: the tailer flushes the turn's last
`assistant_message` *after* the Stop hook, so the tail is usually higher than
the boundary and the wait would block until the *next* turn. After `/wait`
returns, read the transcript with `wait=2` so the tailer catches up.

---

## Permissions

CC runs with `--permission-prompt-tool mcp__po-k__approve`. Anything CC still
asks about becomes a `permission_request` event
(`{request_id, tool, input, timeout_ms}`) and the session shows
`awaiting_input`. Answer within `permission_timeout` (300 s, else auto-deny):

### `POST /sessions/{id}/permission_requests/{req_id}`
`{"behavior": "allow" | "deny", "message": "optional reason"}` → `{"ok": true, "request_id"}`.

---

## Hub: other boxes

The po-k next to the orchestrator remembers remote boxes and reaches them with
the shared fleet token. Host keys are what you passed to connect: a bare name
(`ange` → `http://ange.zrz:13658`; suffix `POK_HOST_SUFFIX`, port `POK_PORT`),
`host:port`, a `http://` base URL, or `local` (this po-k, always available).

### `POST /hosts`
```json
{"host": "jamail-c1",
 "webhook": {"url": "http://127.0.0.1:8644/webhooks/pok", "secret_env": "POK_WEBHOOK_SECRET"},
 "meta": {"platform": "zulip", "chat_id": "stream:eng", "thread_id": "deploy"}}
```
Probes `/health` and `/sessions` on the box (502 with the resolved `base_url`
if unreachable or the token is rejected; 409 with `local_version` /
`remote_version` if the box runs a different po-k build), stores it, and returns
`{"host", "base_url", "version", "sessions": [...], "webhook", "meta"}`. The
webhook and meta become the defaults for watches on this host; `meta` (≤ 2 KB
object) is echoed verbatim in every webhook so the orchestrator can route the
wake-up back to the conversation that asked.

### `GET /hosts` · `GET /hosts/{host}` · `DELETE /hosts/{host}`
List (with `active_watches` counts) · one host plus a live `probe` · forget a
host and stop its watches.

### `ANY /hosts/{host}/sessions` · `ANY /hosts/{host}/sessions/{*rest}`
Transparent proxy: `/hosts/<h>/sessions/<sid>/wait?since=3` is exactly
`/sessions/<sid>/wait?since=3` on the remote box — same body, same response,
same status codes. Long-poll and SSE routes are passed through with generous
timeouts. `502 {"error": "cannot reach host ..."}` when the box is down.

`POST /hosts/{host}/sessions` accepts three extra fields that never reach the
box: `"wake": true` (start a watch with the host's default webhook),
`"webhook": {...}` (explicit target, implies wake), `"meta": {...}` (override).
On `201` the response gains `"watch": {...}`.

### `POST /watches`
`{"host": "jamail-c1", "session_id": "<sid>", "webhook"?: {...}, "meta"?: {...}}` →
`201` watch. Starts from the session's current `boundary_cursor`, so an old
turn never fires. `409 {"error", "watch_id"}` if already watched.

### `GET /watches[?host=&state=]` · `GET /watches/{id}` · `DELETE /watches/{id}`
Watch rows: `{id, host, session_id, webhook, meta, since_boundary, state, last_event, last_error, created_at, updated_at}`.
States: `active`, `done` (session ended / lost), `failed`, `stopped`.

### Webhook contract
The hub long-polls the remote `/wait` and POSTs one signed JSON body per event:

| event | when |
|---|---|
| `finished` | the turn ended (status `idle`) |
| `needs_input` | status `awaiting_input`: a `permission_request` or `user_question` — see `deciding_event.payload` |
| `ended` | the session ended; the watch is done |
| `connection_lost` | the box did not answer three times in a row |
| `connection_restored` | it answers again |
| `session_lost` | the box no longer knows the session (404); done |
| `auth_failed` | the box rejected the fleet token; watch failed |
| `version_mismatch` | the box runs a different po-k build; watch failed |

```json
{"event_type": "pok_notification", "event": "finished", "host": "jamail-c1",
 "session_id": "<sid>", "watch_id": "w-...", "status": "idle", "boundary_cursor": 42,
 "deciding_event": {"kind": "stop", "seq": 42, "ts": "...", "payload": null},
 "message": null, "meta": {"chat_id": "stream:eng"},
 "origin": {"platform": "", "chat_id": "stream:eng", "chat_name": "", "thread_id": "", "user_id": "", "user_name": "", "session_key": "", "hint": ""},
 "ts": "..."}
```
`origin` mirrors `meta` with every routing key always present (empty when
unknown) so a receiver template like `{origin.chat_id}` can never render literally.
Headers: `x-webhook-signature` = hex HMAC-SHA256 of the exact body under the
secret named by `secret_env` (an env var of the `po-k serve` process) or
`secret_file`; `x-request-id` = `<watch_id>:<event>:<boundary_cursor>` (dedupe
key); `x-pok-event: pok_notification`. Bodies never contain CC prose — the woken
turn reads `.../events` itself. Deliveries retry 30 s → 15 m for six attempts;
a `4xx` parks the delivery and records `last_error` on the watch.

---

## Internal (CC calls these)

### `POST /sessions/{id}/hooks/{event}`
Hook ingestion; `hooks.json` curls it with the bearer. `{event}` ∈
`UserPromptSubmit | Stop | SubagentStop | PostToolUse | Notification | SessionEnd`.

### `POST /sessions/{id}/mcp/approve`
Called by `po-k cc-mcp` (the per-session MCP server CC launches) to request a
permission decision; blocks until the orchestrator answers.

---

## Event kind reference

Every event is `{seq, ts, kind, ...payload}`.
Lifecycle: `cc_started`, `cc_exited`, `cc_recovered`, `cc_lost`.
Hooks: `user_prompt`, `stop` (the turn boundary), `subagent_stop`,
`tool_result`, `notification`, `idle_notification` (CC's post-turn "waiting for
input"; not status-relevant), `session_end`.
Transcript (JSONL tailer): `user_prompt`, `assistant_message` (`text`,
`stop_reason`, `turn_id`), `tool_use`, `tool_result`, `user_question`
(AskUserQuestion), `turn_end` (cost/usage), `raw_<type>`.
Permissions: `permission_request`, `permission_decision`.

## Restart behaviour

Sessions live on in zellij across a po-k restart. On startup po-k recovers every
session with `ended_at IS NULL` whose zellij + MCP socket still answer
(`cc_recovered`), marks the rest ended (`cc_lost`), resumes each JSONL tailer
from its stored byte offset, and respawns every active hub watch. The callback
URL baked into a session's `hooks.json` must still match this server: a bind
change orphans older sessions (logged at recovery) — recreate them.
