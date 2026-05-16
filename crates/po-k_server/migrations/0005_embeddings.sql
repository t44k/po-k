-- Dense embeddings per event, used for the dense half of hybrid retrieval.
-- vec is little-endian f32, length = dim (e.g. 384 for bge-small-en-v1.5).
-- model is stored so a future model swap can re-embed only mismatched rows.

CREATE TABLE IF NOT EXISTS events_embedding (
    session_key   TEXT NOT NULL,
    file_relpath  TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    team_id       TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    vec           BLOB NOT NULL,
    model         TEXT NOT NULL,
    embedded_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (session_key, file_relpath, line_no)
);

CREATE INDEX IF NOT EXISTS idx_events_embedding_team
    ON events_embedding(team_id);
