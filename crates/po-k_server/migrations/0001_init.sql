-- po-k M9 schema (fresh from scratch — previous migrations 0001..0005 were
-- collapsed into this single file as part of the M9 wipe-and-rebuild).

PRAGMA foreign_keys = ON;

-- ─── identity ────────────────────────────────────────────────────────────────

CREATE TABLE teams (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE users (
    id          TEXT PRIMARY KEY,
    team_id     TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    slug        TEXT NOT NULL,
    label       TEXT NOT NULL DEFAULT '',
    role        TEXT NOT NULL CHECK (role IN ('admin', 'member')),
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (team_id, slug)
);

CREATE INDEX idx_users_team ON users(team_id);

CREATE TABLE api_keys (
    key_hash      TEXT PRIMARY KEY,
    user_id       TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    label         TEXT NOT NULL DEFAULT '',
    last_used_at  TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_api_keys_user ON api_keys(user_id);

-- ─── projects ────────────────────────────────────────────────────────────────

CREATE TABLE projects (
    id           TEXT PRIMARY KEY,
    team_id      TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    slug         TEXT NOT NULL,
    label        TEXT NOT NULL DEFAULT '',
    description  TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (team_id, slug)
);

CREATE INDEX idx_projects_team ON projects(team_id);

CREATE TABLE project_aliases (
    project_id    TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    cwd_pattern   TEXT NOT NULL,
    PRIMARY KEY (project_id, cwd_pattern)
);

-- ─── machines + sessions + events ───────────────────────────────────────────

CREATE TABLE machines (
    team_id     TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    machine_id  TEXT NOT NULL,
    first_seen  TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (team_id, machine_id)
);

CREATE TABLE sessions (
    session_key       TEXT PRIMARY KEY,
    team_id           TEXT NOT NULL REFERENCES teams(id),
    user_id           TEXT NOT NULL REFERENCES users(id),
    project_id        TEXT REFERENCES projects(id),
    machine_id        TEXT NOT NULL,
    original_cwd      TEXT,                 -- the unsanitized cwd from CC events
    sanitized_cwd     TEXT NOT NULL,
    session_uuid      TEXT NOT NULL,
    first_event_at    TEXT,
    last_event_at     TEXT,
    event_count       INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_sessions_team        ON sessions(team_id, last_event_at DESC);
CREATE INDEX idx_sessions_user        ON sessions(user_id, last_event_at DESC);
CREATE INDEX idx_sessions_project     ON sessions(project_id, last_event_at DESC);

CREATE TABLE events (
    session_key   TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    file_relpath  TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    byte_offset   INTEGER NOT NULL,
    team_id       TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    project_id    TEXT,
    timestamp     TEXT,
    kind          TEXT,
    is_sidechain  INTEGER NOT NULL DEFAULT 0,
    agent_id      TEXT NOT NULL DEFAULT '',
    turn_id       TEXT,                       -- = last-prompt.leafUuid
    original_cwd  TEXT,
    raw           BLOB NOT NULL,
    received_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, file_relpath, line_no)
);

CREATE INDEX idx_events_session_ts    ON events(session_key, timestamp);
CREATE INDEX idx_events_kind          ON events(kind);
CREATE INDEX idx_events_team          ON events(team_id);
CREATE INDEX idx_events_user          ON events(user_id);
CREATE INDEX idx_events_project       ON events(project_id);
CREATE INDEX idx_events_turn          ON events(turn_id);

-- Auxiliary tables that piggyback on session_key (so no team/user/project columns).
CREATE TABLE tool_results (
    session_key  TEXT NOT NULL,
    spill_id     TEXT NOT NULL,
    content      BLOB NOT NULL,
    received_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, spill_id)
);

CREATE TABLE subagent_meta (
    session_key   TEXT NOT NULL,
    agent_file    TEXT NOT NULL,
    agent_type    TEXT,
    description   TEXT,
    PRIMARY KEY (session_key, agent_file)
);

-- ─── live status (heartbeat) ────────────────────────────────────────────────

CREATE TABLE live_sessions (
    session_key       TEXT PRIMARY KEY,
    status            TEXT NOT NULL DEFAULT 'unknown',
    pid               INTEGER,
    started_at        TEXT,
    updated_at        TEXT,
    background_tasks  INTEGER NOT NULL DEFAULT 0,
    active_subagents  INTEGER NOT NULL DEFAULT 0,
    heartbeat_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_live_heartbeat ON live_sessions(heartbeat_at DESC);

-- ─── topics + digests (memory quadrants) ─────────────────────────────────────

CREATE TABLE topics (
    id                   TEXT PRIMARY KEY,
    team_id              TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    scope_kind           TEXT NOT NULL
        CHECK (scope_kind IN ('global','global-project','user','user-project')),
    user_id              TEXT REFERENCES users(id) ON DELETE CASCADE,
    project_id           TEXT REFERENCES projects(id) ON DELETE CASCADE,
    question             TEXT NOT NULL,
    system_prompt_extras TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (
        (scope_kind = 'global'         AND user_id IS NULL AND project_id IS NULL) OR
        (scope_kind = 'global-project' AND user_id IS NULL AND project_id IS NOT NULL) OR
        (scope_kind = 'user'           AND user_id IS NOT NULL AND project_id IS NULL) OR
        (scope_kind = 'user-project'   AND user_id IS NOT NULL AND project_id IS NOT NULL)
    )
);

CREATE INDEX idx_topics_team ON topics(team_id);
CREATE INDEX idx_topics_user ON topics(user_id);
CREATE INDEX idx_topics_project ON topics(project_id);

CREATE TABLE digests (
    topic_id            TEXT PRIMARY KEY REFERENCES topics(id) ON DELETE CASCADE,
    version             INTEGER NOT NULL DEFAULT 1,
    digest_markdown     TEXT NOT NULL DEFAULT '',
    evidence_event_ids  TEXT NOT NULL DEFAULT '[]',
    llm_backend         TEXT NOT NULL DEFAULT '',
    llm_model           TEXT NOT NULL DEFAULT '',
    written_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ─── search index (fts5) + embeddings ───────────────────────────────────────

CREATE VIRTUAL TABLE events_fts USING fts5(
    session_key   UNINDEXED,
    file_relpath  UNINDEXED,
    line_no       UNINDEXED,
    team_id       UNINDEXED,
    user_id       UNINDEXED,
    project_id    UNINDEXED,
    raw,
    tokenize='porter unicode61'
);

CREATE TABLE events_embedding (
    session_key   TEXT NOT NULL,
    file_relpath  TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    team_id       TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    project_id    TEXT,
    vec           BLOB NOT NULL,
    model         TEXT NOT NULL,
    embedded_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, file_relpath, line_no)
);

CREATE INDEX idx_events_embedding_team    ON events_embedding(team_id);
CREATE INDEX idx_events_embedding_user    ON events_embedding(user_id);
CREATE INDEX idx_events_embedding_project ON events_embedding(project_id);
