-- Topics: admin-curated questions whose answers we keep distilled across sessions.
-- A topic's `scope` is either the literal string "team" (covers all sessions in the
-- topic's team) or "project:<sanitized_cwd>" (covers one project). The renderer keeps
-- v1 simple — one digest per topic id; if you want per-project answers, create one
-- topic per project.

CREATE TABLE IF NOT EXISTS topics (
    id                   TEXT PRIMARY KEY,
    team_id              TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    scope                TEXT NOT NULL DEFAULT 'team',
    question             TEXT NOT NULL,
    system_prompt_extras TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_topics_team ON topics(team_id);

-- Digests: the current LLM-summarised answer to a topic, plus provenance.
CREATE TABLE IF NOT EXISTS digests (
    topic_id            TEXT PRIMARY KEY REFERENCES topics(id) ON DELETE CASCADE,
    version             INTEGER NOT NULL DEFAULT 1,
    digest_markdown     TEXT NOT NULL DEFAULT '',
    evidence_event_ids  TEXT NOT NULL DEFAULT '[]',  -- JSON array of (file_relpath, line_no) tuples
    llm_backend         TEXT NOT NULL DEFAULT '',
    llm_model           TEXT NOT NULL DEFAULT '',
    written_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
