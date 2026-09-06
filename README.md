# po-k

*po-k* — from the Hungarian **pók** (spider): one process on every dev box,
legs reaching every Claude Code session, one web an agent can pull on.

Drive fleets of Claude Code (CC) instances over zellij from HTTP, and from an
agent through MCP.

```
 Hermes (agent) ──stdio MCP──▶ po-k mcp ──HTTP──▶ po-k serve @ange:13658 ──HTTP (one fleet token)──▶ po-k serve @<box>.zrz:13658 ──▶ CC in zellij
                 ◀── webhook POST (HMAC) ─────────────┘  hub: hosts · proxy · watchers
```

One binary, `po-k`, four roles:

| command | where | what |
|---|---|---|
| `po-k serve` | every box | the HTTP API for local CC sessions **and** the hub: connect other boxes, proxy their API, watch their sessions and fire webhooks |
| `po-k mcp` | next to the agent | stdio MCP server; talks only to the local `po-k serve` |
| `po-k cc-mcp` | launched by CC | per-session permission shim (internal) |
| `po-k export-profile` | once | v1 Xpo-k profiles → CC plugin directories |

No per-box configuration: a session is fully described by its create request
(directory, model, plugins, MCP servers, agent, system prompt). The only thing
a box needs is the fleet bearer token. New directories are marked trusted in
`~/.claude.json` before CC starts, so any folder works as a session `cwd`.

## Install

```sh
cargo build --release          # target/release/po-k
```

`po-k serve` shells out to `zellij` and `claude`; both must be on `$PATH`.
zellij must be the MCP fork —
[t44k/zellij `mcp-direct-ipc-refactor`](https://github.com/t44k/zellij/tree/mcp-direct-ipc-refactor),
built with `--features mcp_server_capability`, with `mcp { enabled true }` in
its config. `po-k serve` checks this at startup (`zellij mcp --capabilities`
plus a probe session that must expose its socket under
`$XDG_RUNTIME_DIR/zellij/` or `~/.cache/zellij/`) and refuses to start
otherwise.

## Quick start

```sh
# on every box (once): write the token file (fleet key) — config is optional
po-k init --token "$FLEET_TOKEN"        # or: POK_TOKEN=... po-k serve
po-k serve                              # 0.0.0.0:13658

# from anywhere on the box network
TOK=$(cat ~/.config/po-k/auth.token); H="Authorization: Bearer $TOK"
B=http://box.zrz:13658

curl -s $B/docs | jq .schemas.create_session     # what a create body looks like
SID=$(curl -s -H "$H" -d '{
    "cwd": "/workspace",
    "model": "fable",
    "plugins": ["/zirzen/base/plugins/sapi"],
    "mcp_servers": {"linear": {"command": "node", "args": ["/opt/linear-mcp/index.js"], "env": {"LINEAR_ACCESS_TOKEN": "${LINEAR_ACCESS_TOKEN}"}}}
  }' $B/sessions | jq -r .session_id)
CUR=$(curl -s -H "$H" -d '{"text":"Review the auth module."}' $B/sessions/$SID/messages | jq -r .cursor)
curl -s -H "$H" "$B/sessions/$SID/wait?since=$CUR&timeout=600"          # blocks until the turn ends
curl -s -H "$H" "$B/sessions/$SID/messages?offset=-1&size=5&wait=2"     # the reply
curl -s -H "$H" -X DELETE $B/sessions/$SID
```

`GET /help` is the full reference; `GET /docs` is its machine-readable twin
(routes, JSON Schemas for every body, defaults, cursor rules, webhook contract).
Both are public.

## Drive it from Hermes (MCP)

The po-k on the agent's box is the hub. Hermes runs `po-k mcp` over stdio; every
tool takes a `host` (a box name, or `local`).

```yaml
# ~/.hermes/config.yaml
mcp_servers:
  pok:
    command: /usr/local/zirzen/current/bin/po-k
    args: [mcp]
    env:
      POK_URL: http://127.0.0.1:13658                       # the local po-k serve
      POK_TOKEN_FILE: /home/devuser/.config/po-k/auth.token # the fleet key
      POK_WEBHOOK_URL: http://127.0.0.1:8644/webhooks/pok   # Hermes' webhook adapter
      POK_WEBHOOK_SECRET_FILE: /home/devuser/.hermes/pok_webhook_secret  # or POK_WEBHOOK_SECRET_ENV (env of *po-k serve*)
    timeout: 660                                            # `wait` can block for 600 s
    enabled: true
```

Tools (`mcp_pok_*` in Hermes): `docs`, `connect`, `hosts`, `host`, `disconnect`,
`create`, `sessions`, `prompt`, `status`, `wait`, `events`, `pane`, `interrupt`,
`clear`, `upload`, `cost`, `capabilities`, `permission`, `delete`, `watch`,
`unwatch`, `watches`.

Agent flow: `connect(host, meta={chat_id, thread_id})` → `create(host, cwd,
model, plugins, wake=true)` → `prompt(...)` → do other work → a webhook wakes a
Hermes turn with `{event: "finished" | "needs_input" | ..., host, session_id,
boundary_cursor, meta}` → `events(host, session_id, size=10)` → answer or
`permission(...)`. Without a webhook, `wait(host, session_id)` blocks up to
10 minutes and returns the status; call it again on `timed_out`.

## The hub

`POST /hosts {"host": "jamail-c1", "webhook": {"url", "secret_env"}, "meta": {...}}`
probes the box and remembers it. Then every session route is available as
`/hosts/{host}/sessions/...` — a transparent proxy with the same bodies and
status codes. `POST /hosts/{host}/sessions` with `"wake": true` also starts a
**watch**: the hub long-polls the remote `/wait` and POSTs a signed envelope to
the webhook on every boundary.

| event | when |
|---|---|
| `finished` | the turn ended (status `idle`) |
| `needs_input` | a `permission_request` or `user_question` is outstanding |
| `ended` | the session ended; watch done |
| `connection_lost` / `connection_restored` | the box stopped / resumed answering |
| `session_lost` | the box no longer knows the session |
| `auth_failed` | the box rejected the fleet token |
| `version_mismatch` | the box runs a different po-k build |

Headers: `x-webhook-signature` (hex HMAC-SHA256 of the exact body under the
secret named by `secret_env`/`secret_file`), `x-request-id`
(`<watch_id>:<event>:<boundary>`, dedupe key), `x-pok-event: pok_notification`.
Bodies carry metadata only, never CC prose; `meta` is echoed verbatim so the
woken turn knows which conversation asked. Watches are persisted and respawned
when `po-k serve` restarts.

## Plugins instead of profiles

Everything a session should know or be able to do is a CC plugin directory
(`CLAUDE.md`, `agents/`, `skills/`, `.mcp.json`, `hooks/`) or a `.zip`/URL, passed
as `plugins: [...]`. Shared plugins live in the zirzen base layer
(`/zirzen/base/plugins/<name>` inside every box). The v1 Xpo-k profiles were
converted once:

```sh
po-k export-profile --db ~/.config/xpo-k/profiles.db --out ~/c/zirzen/defaults/plugins
# credentials in MCP env/headers become ${NAME}; export them on the box
```

Settings (model, effort, permission mode) cannot ride in a plugin — pass them on
create.

## Cursors

| Cursor | Where | Use |
|---|---|---|
| **tail** | `cursor` on `/status`, `/wait`; `next_cursor` on `/events` | paging: `/events?offset=<tail>` |
| **boundary** | `boundary_cursor` on `/status`, `/wait`; `cursor` from `POST /messages`; every webhook | `/wait?since=<boundary>` |

Never arm `/wait` with a tail cursor: the tailer flushes the turn's last
`assistant_message` after the Stop hook, so the tail is usually higher than the
boundary and the wait would block until the *next* turn. Read the transcript with
`wait=2` after `/wait` returns.

## Permissions

CC runs with `--permission-prompt-tool mcp__po-k__approve`. Anything it still
asks becomes a `permission_request` event, the session shows `awaiting_input`,
a watch fires `needs_input`. Answer with
`POST /sessions/{id}/permission_requests/{req_id} {"behavior": "allow"|"deny"}`
within 300 s (else auto-deny).

## Configuration

`~/.config/po-k/po-k.yaml` — both keys optional, a missing file means defaults:

```yaml
auth:
  bearer_token_file: ~/.config/po-k/auth.token
server:
  bind: 0.0.0.0:13658
```

| env / flag | applies to | meaning |
|---|---|---|
| `POK_CONFIG` / `--config` | serve, init | config file path |
| `POK_BIND` / `--bind` | serve | listen address |
| `POK_TOKEN_FILE` / `--token-file` | serve, mcp | bearer token file |
| `POK_TOKEN` / `--token` | serve, init, mcp | token value; `serve`/`init` write it to the file (0600) |
| `POK_DB` / `--db` | serve | events + hub database (default `~/.config/po-k/events.db`) |
| `POK_URL` | mcp | the local `po-k serve` (default `http://127.0.0.1:13658`) |
| `POK_WEBHOOK_URL`, `POK_WEBHOOK_SECRET_FILE` / `POK_WEBHOOK_SECRET_ENV`, `POK_META` | mcp | defaults for `wake`/`watch`/`connect` |
| `POK_HOST_SUFFIX`, `POK_PORT` | serve (hub) | how a bare box name becomes a URL (`.zrz`, `13658`) |
| `POK_WEBHOOK_SECRET` (or whatever `secret_env` names) | serve (hub) | the HMAC secret, read at send time |

Fixed defaults a request can override per session: model `fable`, effort
`xhigh`, permission mode `bypassPermissions`, permission timeout 300 s, slash
commands disabled, zellij session `po-k-<name>`.

## Version handshake

Every po-k → po-k request (hub → box, `po-k mcp` → local serve) carries
`x-pok-version`; a different build is refused with a 409 naming both versions,
and `POST /hosts` also compares the remote `/health` version. Deploy the same
build everywhere before connecting boxes.

## Security

- One fleet token: every po-k accepts it and the hub uses it to call the
  others. A leak exposes every box — keep it in the base layer at mode 0600.
- `serve` binds `0.0.0.0` because the box network is private. Every route
  except `/health`, `/help`, `/docs` is bearer-protected, including CC's own
  hook and permission callbacks. No TLS: tunnel if a box is ever reachable
  from outside.
- Webhooks are always signed; unsigned targets are refused. Secrets are
  referenced by env var name or file path, never stored or echoed.

## Layout

```
~/.config/po-k/
  po-k.yaml · auth.token
  events.db                          # sessions, events, hub hosts + watches
~/.cache/po-k/sessions/<sid>/
  hooks.json                         # --settings: po-k's hooks + owned settings keys
  mcp.json                           # --mcp-config: request servers + po-k cc-mcp (last)
  system_prompt.md                   # --append-system-prompt-file (when given)
```

CC's transcripts live under `~/.claude/projects/<sanitized-cwd>/`; po-k tails
them, never copies.

## Upgrading from v1 (Xpo-k)

- Delete Xpo-k; the hub replaces routing, and watches + webhooks replace
  subscriptions. There is no profile store: export profiles to plugins.
- The port moved 7070 → 13658 and the callback URL is baked into each running
  session's `hooks.json`, so recreate sessions that were alive during the
  upgrade (recovery logs each one whose URL no longer matches).
- `po-k mcp --session-id …` (the old shim invocation in already-generated
  `mcp.json` files) still works; new sessions use `po-k cc-mcp`.
