-- +migrate Up
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blobs (
    id SERIAL PRIMARY KEY,
    blake3 TEXT NOT NULL UNIQUE,
    mime_type TEXT,
    size BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS file_paths (
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    folder TEXT NOT NULL,
    filename TEXT NOT NULL,
    blob_id INTEGER NOT NULL REFERENCES blobs(id),
    mtime BIGINT NOT NULL,
    last_seen_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, folder, filename)
);

CREATE INDEX IF NOT EXISTS idx_file_paths_blob_id ON file_paths(blob_id);
CREATE INDEX IF NOT EXISTS idx_file_paths_source_id ON file_paths(source_id);
CREATE INDEX IF NOT EXISTS idx_file_paths_filename ON file_paths(filename);
