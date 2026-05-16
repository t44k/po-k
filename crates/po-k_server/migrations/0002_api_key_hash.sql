-- Move api_keys from plaintext to hashed storage. The M1 column `key` held the
-- plaintext (auth was a stub); M3 replaces it with `key_hash` (blake3 hex).
-- Any existing rows are dropped — they were dev-only and the operator should
-- run `po-k_server admin keygen` to mint fresh keys.

DROP TABLE IF EXISTS api_keys;

CREATE TABLE api_keys (
    key_hash    TEXT PRIMARY KEY,
    team_id     TEXT NOT NULL DEFAULT 'default' REFERENCES teams(id),
    label       TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
