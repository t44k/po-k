-- M1 schema. team_id is on every row from day 1 so adding tenants in M3 doesn't reshape tables.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS teams (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO teams (id, label) VALUES ('default', 'default');

CREATE TABLE IF NOT EXISTS api_keys (
    -- Stored as the raw key in M1 (auth is a stub). M3 swaps to a hash column.
    key         TEXT PRIMARY KEY,
    team_id     TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    label       TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS machines (
    -- Composite identity: a single team can have many machines with the same machine_id label
    -- across users; key by (team_id, machine_id).
    team_id     TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    machine_id  TEXT NOT NULL,
    first_seen  TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (team_id, machine_id)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_key      TEXT PRIMARY KEY,
    team_id          TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    machine_id       TEXT NOT NULL,
    sanitized_cwd    TEXT NOT NULL,
    session_uuid     TEXT NOT NULL,
    first_event_at   TEXT,
    last_event_at    TEXT,
    event_count      INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sessions_team ON sessions(team_id, last_event_at DESC);

CREATE TABLE IF NOT EXISTS events (
    session_key   TEXT NOT NULL REFERENCES sessions(session_key),
    file_relpath  TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    byte_offset   INTEGER NOT NULL,
    team_id       TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    timestamp     TEXT,
    kind          TEXT,
    is_sidechain  INTEGER NOT NULL DEFAULT 0,
    agent_id      TEXT NOT NULL DEFAULT '',
    raw           BLOB NOT NULL,
    received_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, file_relpath, line_no)
);

CREATE INDEX IF NOT EXISTS idx_events_session_ts ON events(session_key, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);

-- Placeholders so M2+ migrations land cleanly.
CREATE TABLE IF NOT EXISTS tool_results (
    session_key  TEXT NOT NULL,
    spill_id     TEXT NOT NULL,
    content      BLOB NOT NULL,
    received_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, spill_id)
);

CREATE TABLE IF NOT EXISTS subagent_meta (
    session_key   TEXT NOT NULL,
    agent_file    TEXT NOT NULL,
    agent_type    TEXT,
    description   TEXT,
    PRIMARY KEY (session_key, agent_file)
);
