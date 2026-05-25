# po-k

HTTP service for driving Claude Code instances inside dedicated zellij sessions.

Each target machine runs one `po-k serve`. A remote orchestrator hits its HTTP
API to: list configured projects, spawn a Claude Code session per project (each
in its own zellij session), push prompts, stream events, decide tool-permission
prompts via an MCP round-trip, and tear it all down on `DELETE /sessions/:id`.

## Install

```sh
cargo install --path crates/po-k       # or build with `cargo build --release`
```

`po-k` shells out to `zellij` and `claude`; both must be on `$PATH`. Tested
against zellij 0.44 + Claude Code (any recent version).

## First-run

```sh
po-k init                              # writes ~/.config/po-k/po-k.yaml +
                                       # generates ~/.config/po-k/auth.token

$EDITOR ~/.config/po-k/po-k.yaml       # add your projects under projects:

po-k serve --install-systemd           # writes ~/.config/systemd/user/po-k.service
                                       # and enables it. Drop the flag to just
                                       # run in the foreground.
```

`po-k.yaml` is hot-reloaded — edits to `projects:` show up in `GET /projects`
without a restart.

## Drive it from an orchestrator

```sh
TOK=$(cat ~/.config/po-k/auth.token)
H="Authorization: Bearer $TOK"

# 1. What projects are available?
curl -sH "$H" http://127.0.0.1:7070/projects

# 2. Spawn a session for one of them.
SID=$(curl -sH "$H" -H 'Content-Type: application/json' \
   -d '{"project":"po-k"}' \
   http://127.0.0.1:7070/sessions | jq -r .session_id)

# 3. Send a prompt.
curl -sH "$H" -H 'Content-Type: application/json' \
   -d '{"text":"What does this repo do?"}' \
   http://127.0.0.1:7070/sessions/$SID/messages

# 4. Stream the response. Either:
#    a) long-poll
curl -sH "$H" "http://127.0.0.1:7070/sessions/$SID/events?since=0&wait=30"
#    b) SSE
curl -NsH "$H" "http://127.0.0.1:7070/sessions/$SID/events/stream"

# 5. Stop a running operation.
curl -sH "$H" -X POST http://127.0.0.1:7070/sessions/$SID/interrupt

# 6. Tear down.
curl -sH "$H" -X DELETE http://127.0.0.1:7070/sessions/$SID
```

## Permission round-trip

CC starts with `--permission-mode acceptEdits` + `--permission-prompt-tool
mcp__po-k__approve`. Edits auto-approve; everything else flows through po-k's
MCP and surfaces to the orchestrator:

1. CC calls `mcp__po-k__approve({tool_name, input})`.
2. `po-k mcp` (a subprocess of CC) POSTs to `/sessions/:id/mcp/approve` on the
   po-k server.
3. The server emits a `permission_request` event with a `request_id` and
   blocks the MCP call.
4. Orchestrator answers: `POST /sessions/:id/permission_requests/:req_id`
   `{"behavior":"allow"|"deny", "message":"..."}`.
5. po-k server returns the decision to `po-k mcp`, which returns it to CC.
6. On `cc.permission_timeout` (default 60 s), po-k auto-denies and CC carries
   on. The events table records both the request and the decision.

## HTTP API surface

| Method | Path | Notes |
|---|---|---|
| `GET` | `/health` | unauthenticated liveness |
| `GET` | `/projects` | configured projects + running session_ids |
| `POST` | `/sessions` | `{project}` → spawn |
| `GET` | `/sessions[/:id]` | list / detail |
| `DELETE` | `/sessions/:id` | graceful teardown |
| `POST` | `/sessions/:id/messages` | `{text}` → write to pane (+`\n`) |
| `POST` | `/sessions/:id/interrupt` | ESC into pane |
| `POST` | `/sessions/:id/clear` | `/clear\n` into pane |
| `POST` | `/sessions/:id/files` | `{filename, content_base64}` → `<cwd>/.po-k-inbox/<name>` |
| `GET` | `/sessions/:id/events?since=<seq>&wait=<sec>` | cursor long-poll (max wait 60 s) |
| `GET` | `/sessions/:id/events/stream` | SSE |
| `GET` | `/sessions/:id/cost` | aggregate from `turn_end` events |
| `POST` | `/sessions/:id/permission_requests/:req_id` | orchestrator decides |
| `POST` | `/sessions/:id/hooks/:event` | called by CC's per-session `--settings` hooks (internal) |
| `POST` | `/sessions/:id/mcp/approve` | called by `po-k mcp` subprocess (internal) |

All authenticated endpoints require `Authorization: Bearer <token>`.

## Configuration

Annotated example (your edited `po-k.yaml`):

```yaml
server:
  bind: 127.0.0.1:7070
  base_url: http://127.0.0.1:7070    # what the hook curl + po-k mcp call back at
  reload_on_change: true

auth:
  bearer_token_file: ~/.config/po-k/auth.token

cc:                                  # defaults; per-project overrides allowed
  model: sonnet
  effort: medium
  permission_mode: acceptEdits
  permission_timeout: 60s
  disable_slash_commands: true

zellij:
  session_prefix: po-k-              # session name = <prefix><project>

projects:
  - name: po-k
    cwd: /workspace
  - name: dotfiles
    cwd: /home/me/dotfiles
    model: claude-opus-4-7           # per-project override
```

## Security

- Default bind is `127.0.0.1`; non-loopback binds log a one-time WARN. Tunnel
  through SSH / Tailscale / WireGuard — po-k does **not** terminate TLS.
- `auth.token` is 64 hex chars, generated by `po-k init` at mode 0600.
- The token is baked into per-session `hooks.json` (so CC's hook curls
  authenticate themselves) and read by `po-k mcp` from the token file path
  passed in its argv. It is **not** placed on any command line.

## Layout

```
~/.config/po-k/
  po-k.yaml
  auth.token
  events.db                          # sqlite, one row per event per session

~/.cache/po-k/sessions/<sid>/
  hooks.json                         # passed to claude --settings
  mcp.json                           # passed to claude --mcp-config
```

CC's transcripts continue to live under `~/.claude/projects/<sanitized-cwd>/`;
po-k only tails them, never copies.
