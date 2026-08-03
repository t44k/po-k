# po-k & Xpo-k

*po-k* — from the Hungarian **pók** (spider): one process at the centre of a
web of Claude Code sessions, legs reaching every machine.

Drive fleets of Claude Code (CC) instances over zellij, from a single HTTP API,
across any number of machines.

Two binaries:

- **`po-k`** runs on each dev box/container. It manages CC processes (spawns one
  per project inside its own zellij session, tails transcripts, derives status,
  brokers tool-permission prompts). It has **no orchestrator-facing HTTP
  server** — it is a WebSocket *client* that dials out to Xpo-k, plus a tiny
  localhost-only listener for CC's own callbacks.
- **`Xpo-k`** ("cross po-k") runs centrally. It is the **only** HTTP entry
  point: it stores composable **profiles**, keeps a live registry of connected
  po-k instances, and routes every orchestrator call to the right po-k over the
  WebSocket — so po-k boxes need only outbound connectivity (NAT/firewall
  friendly, no exposed ports).

```
   orchestrator (Hermes/Ange/curl)
        │  HTTP  (the only HTTP in the system)
        ▼
     ┌───────┐    profiles (SQLite) · po-k registry · merge engine
     │ Xpo-k │
     └───┬───┘
         │  WebSocket  (po-k dials out; request/response framed over WS)
   ┌─────┼───────────────┬───────────────┐
   ▼     ▼               ▼               ▼
 ┌────┐┌────┐         ┌────┐          ┌────┐
 │po-k││po-k│  …      │po-k│   …      │po-k│   (one per machine)
 │ CC ││ CC │         │ CC │          │ CC │
 └────┘└────┘         └────┘          └────┘
```

## Install

```sh
cargo build --release          # builds target/release/{po-k,xpo-k}
# or per-binary:
cargo install --path crates/po-k
cargo install --path crates/xpo-k
```

`po-k` shells out to `zellij` and `claude`; both must be on `$PATH`. Tested
against zellij 0.44 + Claude Code (any recent version). Xpo-k has no external
runtime deps.

## Quick start

### 1. Start Xpo-k (central)

```sh
xpo-k init                              # writes ~/.config/xpo-k/xpo-k.yaml +
                                        # generates ~/.config/xpo-k/auth.token
$EDITOR ~/.config/xpo-k/xpo-k.yaml      # set bind, default_profiles, etc.
xpo-k serve                             # HTTP + WebSocket on 0.0.0.0:8080
```

### 2. Start po-k on each machine

```sh
po-k init                               # writes ~/.config/po-k/po-k.yaml +
                                        # generates ~/.config/po-k/auth.token
$EDITOR ~/.config/po-k/po-k.yaml        # add projects: and the xpok: block
po-k serve --install-systemd            # or run in the foreground
```

The `xpok:` block points po-k at the central server:

```yaml
xpok:
  url: ws://xpo-k.host:8080/ws
  token: "<the xpo-k bearer token>"     # from ~/.config/xpo-k/auth.token
  reconnect_interval: 5s
```

On connect, po-k registers its projects + live sessions. Confirm with
`GET /registry` on Xpo-k. `po-k.yaml` is hot-reloaded — project changes
propagate to Xpo-k without a restart.

## Profiles

A **profile** is a JSON blob describing a complete or partial CC configuration:
`claude_md`, `agents`, `skills`, `mcp_servers`, `hooks`, and `settings`. Profiles
live on Xpo-k and are **composed** — you pick several, Xpo-k merges them in
order, and po-k assembles the result into a CC plugin directory on session start.

```sh
TOK=$(cat ~/.config/xpo-k/auth.token); H="Authorization: Bearer $TOK"

# Create a profile.
curl -sH "$H" -H 'Content-Type: application/json' -d '{
  "name": "base-coding",
  "claude_md": "# Coding Standards\n- 2-space indent\n- conventional commits",
  "skills": { "tdd": { "description": "TDD workflow", "content": "..." } },
  "settings": { "effort": "high" }
}' http://xpo-k.host:8080/profiles

# Preview the merged result of several profiles (no session created).
curl -sH "$H" -H 'Content-Type: application/json' \
  -d '{"profiles":["base-coding","code-reviewer"]}' \
  http://xpo-k.host:8080/profiles/merge
```

**Merge rules** (applied left→right): `claude_md` concatenates with a
`## From profile: <name>` header per section; `agents`/`skills`/`mcp_servers`/
`hooks` union by name (later wins on collision); `settings` deep-merge (later
wins); `tags` deduplicate. Each profile's CLAUDE.md, skills, and agents become
real files in `~/.cache/po-k/sessions/<sid>/plugin/`, passed to CC via
`--plugin-dir`. po-k always injects its own permission MCP server + lifecycle
hooks, which a profile can never override.

**Live updates:** `PUT /profiles/{name}` pushes the re-merged profile to every
running session that uses it. CLAUDE.md and skills hot-reload automatically (CC
watches the files); agent/MCP/hook changes trigger a `/reload-plugins` nudge.

## Drive it from an orchestrator

All calls go to **Xpo-k**; it routes to the owning po-k over WebSocket. The
session API is identical to po-k's old HTTP API, so existing orchestrators just
re-point at Xpo-k.

```sh
TOK=$(cat ~/.config/xpo-k/auth.token); H="Authorization: Bearer $TOK"
X=http://xpo-k.host:8080

# 1. What projects are available across all connected po-k instances?
curl -sH "$H" $X/projects

# 2. Spawn a session with a composed profile + a chosen main agent.
SID=$(curl -sH "$H" -H 'Content-Type: application/json' -d '{
   "project": "acme-api",
   "profiles": ["base-coding", "code-reviewer"],
   "agent": "lead-reviewer",
   "cc_flags": { "model": "opus", "effort": "high" }
 }' $X/sessions | jq -r .session_id)

# 3. Inspect what that session can actually do (agents/skills/MCP it has).
curl -sH "$H" $X/sessions/$SID/capabilities

# 4. Send a prompt.
curl -sH "$H" -H 'Content-Type: application/json' \
   -d '{"text":"Review the auth module for security issues."}' \
   $X/sessions/$SID/messages

# 5. Stream the response — long-poll or SSE.
#    offset/size are required; follow=1 turns a cursor-less tail into a
#    long-poll for NEW events only (see "Cursors" below).
curl -sH "$H" "$X/sessions/$SID/events?offset=-1&size=10&wait=30&follow=1"
curl -NsH "$H" "$X/sessions/$SID/events/stream"

# 6. Block until CC reaches a NEW turn boundary. `since` is the BOUNDARY
#    cursor — the one POST /messages returned, or boundary_cursor from
#    /status or a previous /wait.
curl -sH "$H" "$X/sessions/$SID/wait?since=$CURSOR&timeout=120"

# 7. Interrupt / tear down.
curl -sH "$H" -X POST   $X/sessions/$SID/interrupt
curl -sH "$H" -X DELETE $X/sessions/$SID
```

A plain `{"project":"..."}` body (no `profiles`) still works — it spawns CC with
project-local config only, exactly as before profiles existed.

## Cursors

Three different cursors travel through this API. Mixing them up is the classic
source of "the orchestrator never noticed the turn finished".

| Cursor | Where it comes from | What it means | Use it for |
|---|---|---|---|
| **tail cursor** | `cursor` on `/status` and `/wait`; `next_cursor` on `/events` | highest event `seq` persisted so far | paging forward: `/events?offset=<tail>` |
| **boundary cursor** | `boundary_cursor` on `/status` and `/wait`; `cursor` from `POST /messages` | `seq` of the *deciding* turn-boundary event (the `stop` / notification / lifecycle event) | `/wait?since=<boundary>` |
| **subscription cursor** | `cursor` on a subscription; advanced by **ack** only | how far a subscriber has consumed | nothing manual — the server owns it |

Rules:

- **`/wait?since=` takes the boundary cursor, never the tail.** The two differ
  routinely: the JSONL tailer flushes a turn's final `assistant_message` *after*
  the Stop hook, so the tail is usually higher than the boundary. Re-arming with
  the tail blocks until the *next* turn even though the session is already idle.
- **Never re-arm `/wait` with `next_cursor` from `/events`.** That is a tail
  cursor.
- **`since=0` means "any past boundary counts"** — a stop from a previous turn
  satisfies the wait instantly and looks like a fresh completion. Arm with the
  cursor `POST /messages` gave you (captured *before* the prompt was written), or
  with `boundary_cursor`. The `pok_wait` tool resolves the current
  `boundary_cursor` automatically when you omit `since`.
- **A plain tail read (`offset=-1`) returns immediately and ignores `wait`** once
  a session has any events — it is "give me the latest N", not a subscription.
  To watch for new output, either page forward with `offset=<next_cursor>` or
  pass `follow=1`, which pins the request to the current cursor and long-polls.

## Background notifications

`/wait` only helps while a call is in flight. An orchestrator that has to handle
other work (or ends its turn) needs completions to survive the gap, so Xpo-k
keeps the interest itself:

```sh
# 1. Subscribe BEFORE prompting. The cursor defaults to the session's current
#    event seq, so the subscription can neither miss this turn's stop nor fire
#    on history. (Pass "cursor": 0 to include everything po-k still holds.)
SUB=$(curl -sH "$H" -H 'Content-Type: application/json' \
  -d "{\"session_id\":\"$SID\",\"subscriber\":\"ange\"}" \
  $X/subscriptions | jq -r .subscription_id)

# 2. Send the long task, then go do something else entirely.
curl -sH "$H" -H 'Content-Type: application/json' \
  -d '{"text":"Refactor the payment module and run the suite."}' \
  $X/sessions/$SID/messages

# 3. Later — or from another process — collect what happened. Reading does not
#    consume; `wait` long-polls (max 60s) when nothing is queued yet.
curl -sH "$H" "$X/notifications?subscriber=ange&wait=30"

# 4. Ack what you acted on. Unacked notifications are redelivered, so nothing
#    is lost if you crash in between. Acking advances the subscription cursor
#    and refreshes its TTL.
curl -sH "$H" -H 'Content-Type: application/json' \
  -d '{"ids":["ntf-…"]}' $X/notifications/ack

curl -sH "$H" "$X/subscriptions?subscriber=ange"      # what am I watching?
curl -sH "$H" -X DELETE "$X/subscriptions/$SUB"       # stop watching
```

Contract:

- **Server-owned.** Subscriptions and queued notifications live in Xpo-k's
  SQLite, so they survive an idle orchestrator, an Xpo-k restart, and a po-k
  reconnect.
- **What fires.** Event kinds `stop`, `session_end`, `cc_exited`,
  `notification`, `user_question`, `permission_request` (override with
  `kinds`), plus derived-status changes to `idle`, `awaiting_input`, `ended`
  (override with `statuses`). The status path is a level-triggered safety net:
  it still fires when the sequenced event that caused the transition never
  reached Xpo-k.
- **At-least-once, deduplicated.** Sequenced events are unique per
  `(subscription, seq, kind)`, so a duplicate push or a reconnect replay cannot
  double-deliver. A status notification is suppressed while an unacked one for
  the same status is already queued.
- **Reconnect replay.** When a po-k registers, Xpo-k replays the events it
  persisted while the uplink was down (via the existing `/events` page API,
  from each subscription's cursor) — bounded to the most recent 200 events per
  session.
- **Expiry.** Subscriptions default to a 24 h TTL (max 7 days), refreshed on
  every ack; expired ones and their queued rows are swept automatically.

**Hermes plugin usage.** `pok_subscribe` → do other work → `pok_notifications
(action="poll")` → `pok_notifications(action="ack", ids=[…])`. The plugin
deliberately does **not** spawn a background thread to inject into the
conversation: `ctx.inject_message` is unavailable in gateway mode, so a
thread-based design would drop notifications exactly where they matter most.
Instead the queue is authoritative on the server and polled explicitly — from
the agent's own turn, or from a gateway-side heartbeat/cron that polls
`/notifications` and starts a turn when something is pending. Either way an
unacked notification is never lost.

## Permission round-trip

CC starts with `--permission-mode <mode>` + `--permission-prompt-tool
mcp__po-k__approve`. Edits auto-approve (in `acceptEdits`); everything else flows
through po-k and surfaces to the orchestrator:

1. CC calls `mcp__po-k__approve({tool_name, input})`.
2. `po-k mcp` (a CC subprocess) POSTs to po-k's **localhost** hook listener at
   `/sessions/:id/mcp/approve`.
3. po-k emits a `permission_request` event (forwarded to Xpo-k) with a
   `request_id`, and blocks the MCP call.
4. Orchestrator answers Xpo-k: `POST /sessions/:id/permission_requests/:req_id`
   `{"behavior":"allow"|"deny","message":"..."}`; Xpo-k routes it to po-k.
5. po-k returns the decision to `po-k mcp`, which returns it to CC.
6. On `cc.permission_timeout` (default 60 s) po-k auto-denies and CC carries on.
   Both the request and decision are recorded as events.

## Xpo-k HTTP API

All endpoints except `/health` require `Authorization: Bearer <xpo-k token>`.

**Profiles & registry (served by Xpo-k):**

| Method | Path | Notes |
|---|---|---|
| `GET` | `/health` | unauthenticated; Xpo-k version + connected po-k count |
| `GET` | `/registry` | connected po-k instances, their projects + sessions |
| `GET` | `/profiles` | list (name, version, description, tags) |
| `GET`/`POST` | `/profiles` · `/profiles/{name}` | CRUD (POST create, GET fetch) |
| `PUT`/`DELETE` | `/profiles/{name}` | update (pushes live) / delete |
| `GET` | `/profiles/{name}/history` | version history |
| `POST` | `/profiles/merge` | `{profiles:[...]}` → merged profile (not stored) |
| `POST` | `/profiles/preview` | merge + capabilities preview for a project |
| `POST` | `/subscriptions` | `{session_id, subscriber?, kinds?, statuses?, ttl_secs?, cursor?}` → watch a session |
| `GET` | `/subscriptions[?subscriber=&session_id=]` | list subscriptions |
| `DELETE` | `/subscriptions/{id}` | unsubscribe (drops its queued notifications) |
| `GET` | `/notifications[?subscriber=&session_id=&limit=&wait=]` | pending notifications; reading does not consume |
| `POST` | `/notifications/ack` | `{ids:[...]}` → acknowledge (idempotent) |

**Session API (routed to the owning po-k over WebSocket):**

| Method | Path | Notes |
|---|---|---|
| `GET` | `/projects` | fan-out + merge across all po-k instances |
| `POST` | `/sessions` | `{project, profiles?, agent?, cc_flags?, bare?}` → spawn |
| `GET` | `/sessions` | fan-out list |
| `GET`/`DELETE` | `/sessions/:id` | detail / teardown |
| `POST` | `/sessions/:id/messages` | `{text}` → write to pane; returns the **boundary cursor** to arm `/wait` with |
| `GET` | `/sessions/:id/messages?offset=&size=[&wait=&follow=]` · `/messages/stream` | transcript poll / SSE |
| `POST` | `/sessions/:id/interrupt` · `/clear` | ESC / `/clear` into pane |
| `POST` | `/sessions/:id/files` | `{filename, content_base64}` → `<cwd>/.po-k-inbox/` |
| `GET` | `/sessions/:id/events?offset=&size=[&wait=&follow=]` · `/events/stream` | event poll / SSE; `follow=1` = long-poll for new events |
| `GET` | `/sessions/:id/cost` · `/status` · `/wait` · `/pane` | derived views; `/status` + `/wait` return `cursor` (tail) **and** `boundary_cursor` |
| `GET` | `/sessions/:id/capabilities` | agents/skills/MCP the session actually has |
| `POST` | `/sessions/:id/permission_requests/:req_id` | orchestrator decides |

## Configuration

### `xpo-k.yaml`

```yaml
server:
  bind: 0.0.0.0:8080
  base_url: http://xpo-k.host:8080

auth:
  bearer_token_file: ~/.config/xpo-k/auth.token

default_profiles: []                   # applied to every session
project_defaults:                      # per-project default profiles
  acme-api:
    default_profiles: [base-coding, acme-standards]
```

### `po-k.yaml`

```yaml
auth:
  bearer_token_file: ~/.config/po-k/auth.token

xpok:                                  # the central router (omit to run unmanaged)
  url: ws://xpo-k.host:8080/ws
  token: "<xpo-k bearer token>"
  reconnect_interval: 5s

hooks:
  bind: 127.0.0.1:7070                 # localhost-only CC callback listener (no auth)

cc:                                    # defaults; per-project overrides allowed
  model: sonnet
  effort: medium
  permission_mode: bypassPermissions
  permission_timeout: 60s
  disable_slash_commands: true

zellij:
  session_prefix: po-k-                # session name = <prefix><project>

projects:
  - name: acme-api
    cwd: /workspace
  - name: dotfiles
    cwd: /home/me/dotfiles
    model: claude-opus-4-7             # per-project override
```

## Security

- **Xpo-k** is the only authenticated HTTP surface. Default bind is
  `0.0.0.0:8080`; put it behind a tunnel/reverse proxy — neither binary
  terminates TLS. Tokens are 64 hex chars, mode 0600, generated by `init`.
- **po-k** exposes no orchestrator HTTP. Its single listener binds `127.0.0.1`
  and is unauthenticated *by design* (local trust boundary) — it only accepts
  CC's hook + permission callbacks from the same machine.
- The po-k↔Xpo-k WebSocket is authenticated with the Xpo-k bearer token; po-k
  needs only outbound connectivity.

## Layout

```
~/.config/xpo-k/                       # central
  xpo-k.yaml · auth.token
  profiles.db                          # profiles + version history + session registry

~/.config/po-k/                        # per machine
  po-k.yaml · auth.token
  events.db                            # sqlite, one row per event per session

~/.cache/po-k/sessions/<sid>/
  plugin/                              # generated from the merged profile:
    .claude-plugin/plugin.json
    agents/*.md · skills/*/SKILL.md · CLAUDE.md
    .mcp.json · hooks/hooks.json       # po-k's own MCP + hooks merged in
  # (profile-less sessions instead get flat hooks.json + mcp.json here)
```

CC's transcripts continue to live under `~/.claude/projects/<sanitized-cwd>/`;
po-k only tails them, never copies.
