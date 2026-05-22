# po-k — remaining work

Snapshot of what's NOT yet built, after M1 → M5 + M7 (admin UI) + M8 (live transcript). All milestones below are queued; pick up in roughly the listed order.

## M6 — ticketing bridge (largest remaining unit, ~2 weeks)

Ingest Jira / Linear / Asana, correlate to PRs / commits, expose via MCP. Sketch from the original plan:

- **Ingestors** behind a `TicketSource` trait: `Jira` (PAT, polling + webhook in v2), `Linear` (PAT + GraphQL, webhooks), `Asana` (PAT, polling). Normalise to `Ticket { id, title, body, status, assignee, refs[], created, updated, raw }`. Hand-roll with `reqwest` — none of the per-platform Rust crates do enough to justify the dependency.
- **Ref extraction** per ticket: native platform links first (Jira dev panel via `/rest/dev-status/...`, Linear's `attachmentLinkGitHub`, Asana GitHub app) *plus* regex over body / commit messages (`PROJ-123`, `closes #123`, `Refs:`).
- **Commit ↔ ticket map**: on git ingest (separate cron over registered repos), walk `git log`, attach ticket refs, store `(repo, sha, ticket_id, files_changed[])` in `commit_tickets`.
- **Blame query**: given `(repo, path, optional line range)` → `git blame --porcelain` → SHAs → `tickets`.
- **MCP tools**: `tickets_for_path(repo, path)`, `find_related_tickets(query, k)` (BM25 over ticket body + linked diff; semantic over diff is v6.1).
- Squash-merge / rebase: store both pre- and post-merge SHA when known; accept that some history is unrecoverable.

Schema: `tickets`, `ticket_links` (ticket ↔ commit sha), `repos`. Same `team_id` everywhere.

The trap is diff-aware similarity. Defer it. Blame-based lookup is deterministic and demoable — ship that first.

## live-transcript follow-ups

Built in M8 but with deliberate v1 limits worth filling in:

- **Live sidechain (subagent) events over WS.** Currently subagent events accumulate in the db and only appear (properly collapsed under their parent `Agent` tool_use) on page refresh. Right design: when a subagent event lands, publish to the parent session's bus tagged with `data-agent-id="…"`; client-side, look up or create the matching `<details class="subagent">` block and append.
- **Live tool_use ↔ tool_result pairing.** Today they render as two separate blocks in the live feed. Need: a stable id on the rendered `<details class="tool">` so the result event can find and append into its `<details>` body instead of starting a new block.
- **WS lag recovery.** On `RecvError::Lagged` we send a one-line comment telling the client to refresh. Better: a `since=<line_no>` query the WS client can send to get the missed slice without a full page reload.
- **Pagination tunables.** PAGE_SIZE=200 / OLDER_PAGE_SIZE=100 are fine for sub-10k-event sessions. Adapt to per-session size before rendering huge transcripts on small machines.

## LLM backend follow-ups

The `Llm` trait shipped in M5 with only `ClaudeCli`. Add:

- **`Anthropic`**: direct `reqwest` to Claude API (`POST /v1/messages`). Wire prompt caching for the recurring system prompt + per-topic context window — distillation reruns are the obvious cache hit.
- **`OpenAi`**: direct `reqwest` to the Responses API. Same shape.
- **Per-topic backend override**: `topics.llm_backend` column so an admin can pin one topic to `anthropic` (faster, cached) and another to `claude-cli` (zero-config).

## hybrid retrieval follow-ups

M4.3+M4.4 shipped with brute-force cosine — fine up to ~100k events. When that bites:

- Migrate to `sqlite-vec` (the extension, not just the crate's helpers); replace the loop in `search::dense_topk` with a `MATCH` over `events_vec`.
- Re-embed when the model label in `events_embedding` differs from the running embedder — already a column, not yet used.

## operational / polish

- **Integration tests.** axum `TestServer` covers /ingest, /api/search, /mcp, /ui/admin/keys + /ui/admin/topics. A second collector → server round-trip test in tests/.
- **CSRF on admin.** Trusted-networks caveat from M7 still applies. Per-session token in the cookie, validated on every `POST /ui/admin/*`. Cheap.
- **Auth on /ui/session and /ui/search.** Currently open. Same cookie-or-API-key gate as admin, but allow per-team scoping (read your team's data only). Decide whether transcripts are team-private or team-shared.
- **tokio-cron-scheduler for nightly distillation.** Today distillation is manual (CLI or `/ui/admin/topic/distill`). Add a `distill_at` column per topic and an in-process scheduler.
- **Subagent meta sidecar live updates.** When `agent-*.meta.json` is shipped via `/ingest/subagent-meta`, also publish a small payload on the parent session's bus so the live transcript can upgrade an in-flight `Agent` tool_use into the labelled subagent block.
- **Hooks-based collector mode.** As an alternative to the JSONL tail, write a Claude Code `Stop` / `SubagentStop` / `UserPromptSubmit` hook that POSTs directly to `/ingest`. Lower latency (sub-second), but limited to events Claude Code surfaces via hooks; tail covers the rest.

## not on the roadmap (yet)

- Multi-user under one team (per-user feedback memory vs. team memory) — current model is single-pool-per-team.
- Webhooks back out of po-k (e.g. "digest updated" → Slack).
- Plugin model for additional MCP tools beyond the bundled five.
