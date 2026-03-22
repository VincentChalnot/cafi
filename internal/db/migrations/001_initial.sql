-- +migrate Up
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blobs (
    blake3 TEXT PRIMARY KEY,
    mime_type TEXT,
    size BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS file_paths (
    source_id TEXT NOT NULL REFERENCES sources(id),
    path TEXT NOT NULL,
    blake3 TEXT NOT NULL REFERENCES blobs(blake3),
    mtime BIGINT NOT NULL,
    last_seen_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, path)
);

CREATE INDEX IF NOT EXISTS idx_file_paths_blake3 ON file_paths(blake3);
CREATE INDEX IF NOT EXISTS idx_file_paths_source_id ON file_paths(source_id);
