-- BM25 search over event raw text via fts5.
--
-- Contentless variant: fts5 holds its own copies of the join columns + the indexed text.
-- Storage cost is roughly 1× raw text size; OK for v1 dev volumes. If this ever becomes
-- a problem we can switch to fts5 external content over an `id` column on events.

CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    session_key  UNINDEXED,
    file_relpath UNINDEXED,
    line_no      UNINDEXED,
    team_id      UNINDEXED,
    raw,
    tokenize='porter unicode61'
);

-- Backfill any events that pre-date the fts5 table.
INSERT INTO events_fts (session_key, file_relpath, line_no, team_id, raw)
SELECT session_key, file_relpath, line_no, team_id, CAST(raw AS TEXT)
FROM events
WHERE NOT EXISTS (
    SELECT 1 FROM events_fts f
    WHERE f.session_key = events.session_key
      AND f.file_relpath = events.file_relpath
      AND f.line_no = events.line_no
);
