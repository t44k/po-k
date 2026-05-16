# po-k

A small Rust system that collects, displays, searches, and distills Claude Code session logs across a team.

Three components, one Cargo workspace:

- **`po-k_collector`** — daemon that tails the JSONL session files Claude Code writes under `~/.claude/projects/**`, ships every event (user prompts, model output, tool calls, full subagent transcripts, meta sidecars) to the server.
- **`po-k_server`** — axum + SQLite. Ingests collector batches, serves a transcript UI with collapsed tool calls / subagents, exposes BM25 search, runs the distillation loop, and bundles an MCP server.
- **MCP tools** — `search_sessions`, `list_projects`, `recent_sessions`, `list_topics`, `recall_topic`. Reachable at `POST /mcp` (JSON-RPC 2.0).

## Quick start

```shell
# 1. Build
cargo build --workspace

# 2. Server
./target/debug/po-k_server serve --db /tmp/po-k.db
# default bind: 0.0.0.0:8787; override with --listen 127.0.0.1:8787 for loopback only.

# 3. Mint an API key
KEY=$(./target/debug/po-k_server admin keygen --db /tmp/po-k.db --label my-laptop | head -1)
echo "$KEY"  # shown ONCE; only blake3(key) is stored

# 4. Run the collector (defaults to backfill + live tail)
./target/debug/po-k_collector \
  --api-key "$KEY" \
  --machine-id my-laptop \
  --server-url http://127.0.0.1:8787
# --once  : backfill and exit
# --projects-root /path : override ~/.claude/projects

# 5. Browse
open http://127.0.0.1:8787/ui
```

## Admin commands

```shell
po-k_server admin keygen   --db DB [--team T] [--label L]    # mint
po-k_server admin list-keys --db DB [--team T]               # list (no plaintext)
po-k_server admin revoke   --db DB --label L                 # delete by label

po-k_server admin topic add    --db DB --id ID --question "…" [--scope team|project:CWD] [--team T] [--extras "…"]
po-k_server admin topic list   --db DB [--team T]
po-k_server admin topic remove --db DB --id ID

po-k_server admin distill --db DB [--id ID] [--backend claude-cli] [--model claude-opus-4-7]
# Runs the topic distillation loop now. With no --id, processes every topic.
# Default backend is `claude-cli` which shells out to `claude -p` (zero-config:
# reuses the operator's existing Claude Code auth).
```

## Search

- `/ui/search?q=…` — server-rendered HTML, all teams visible, `<mark>` highlights, source badges (bm25 / dense / both).
- `/api/search?q=…&limit=N` — JSON, requires `X-Api-Key`, scoped to the key's team.
- Hybrid retrieval: BM25 (sqlite fts5) + dense (fastembed-rs `bge-small-en-v1.5`, 384-dim, brute-force cosine over `events_embedding` BLOBs) fused with Reciprocal Rank Fusion (k=60). The fastembed model is downloaded on first run (~80MB into `~/.cache/fastembed`); if the load fails the server degrades to BM25-only.

## Admin UI

Everything in the `admin` CLI subcommands is also reachable in the browser:

- `/ui/login` — log in with any existing API key (sets a HttpOnly cookie).
- `/ui/admin` — dashboard: events, embeddings %, machines, sessions, keys, topics.
- `/ui/admin/keys` — mint + revoke keys (plaintext shown once, only blake3 stored).
- `/ui/admin/topics` — add topics, trigger distillation in the background, read digests.
- `/ui/admin/mcp` — paste-ready Claude Code wiring with a one-click key for a new device.

The admin UI is gated behind a cookie that holds your API key. The public pages (`/ui`, `/ui/project/*`, `/ui/session/*`, `/ui/search`) stay open.

> **No CSRF in v1.** Admin is intended for trusted networks. Run behind a VPN or reverse-proxy auth before opening it to the wider internet.

## MCP wiring (Claude Code)

The `/ui/admin/mcp` page generates a paste-ready snippet with your server URL pre-filled and a freshly-minted device key. The structure is:

`~/.claude/mcp_servers.json`:

```json
{
  "mcpServers": {
    "po-k": {
      "transport": "http",
      "url": "http://127.0.0.1:8787/mcp",
      "headers": { "X-Api-Key": "pk_…" }
    }
  }
}
```

Once configured and Claude Code is restarted, the agent can call `mcp__po-k__recall_topic("auth-pattern")`, `mcp__po-k__search_sessions(query: "…", limit: 10)`, etc. All tool results are scoped to the team this key belongs to.

## Layout

```
crates/
  po-k_core/        Event, SessionKey   (raw-line preservation, blake3 session ids)
  po-k_proto/       NDJSON wire format  (BatchHeader + Event lines, SubagentMetaRow)
  po-k_collector/   notify-debouncer watcher, watermark store, batched HTTP shipper
  po-k_server/      axum app: /ingest, /ui, /api/*, /mcp
                    askama templates, fts5 indexer, distillation loop, admin CLI
```

## Status

| | |
|---|---|
| M1 | collector + server skeleton, end-to-end round-trip ✓ |
| M1 gaps | watermarking, live tail, subagent meta sidecars ✓ |
| M2 | transcript UI (projects → sessions → collapsed tool/subagent transcript) ✓ |
| M3 | real auth, hashed API keys, multi-team isolation ✓ |
| M4.1+4.2 | fts5 BM25 search (UI + JSON API) ✓ |
| M4.3+4.4 | hybrid retrieval (fastembed-rs + brute-force cosine + RRF) ✓ |
| M4.5 | bundled MCP server (5 tools) ✓ |
| M5 | topic-pinned distillation loop with `claude -p` backend ✓ |
| M6 | ticketing bridge (Jira / Linear / Asana ↔ git blame) — pending |

## Notes

- The collector's session ID is `blake3(machine_id || sanitized_cwd || session_uuid)` — same source file from two machines stays distinct.
- The server's ingest is idempotent: `INSERT OR IGNORE` on `(session_key, file_relpath, line_no)`. Re-running the collector is always safe.
- API keys are stored as `blake3(key)`; the plaintext is shown once during `admin keygen` and unrecoverable afterwards.
- `claude -p` runs as a child process — `claude` must be on `PATH` for the default distillation backend.
