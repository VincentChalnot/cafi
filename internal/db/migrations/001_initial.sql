-- v1 initial migration (replaces all previous)

CREATE TABLE IF NOT EXISTS users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sources (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    strategy SMALLINT NOT NULL DEFAULT 0,
    path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, code)
);

CREATE TABLE IF NOT EXISTS tokens (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    expire_at TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS token_sources (
    token_id INT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    source_id INT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    PRIMARY KEY (token_id, source_id)
);

CREATE TABLE IF NOT EXISTS blobs (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    blake3 TEXT NOT NULL UNIQUE,
    mime_type TEXT,
    size BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS file_paths (
    source_id INT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    folder TEXT NOT NULL,
    filename TEXT NOT NULL,
    blob_id INT NOT NULL REFERENCES blobs(id),
    mtime BIGINT NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, folder, filename)
);

CREATE INDEX IF NOT EXISTS idx_file_paths_blob_id ON file_paths(blob_id);
CREATE INDEX IF NOT EXISTS idx_file_paths_source_id ON file_paths(source_id);
CREATE INDEX IF NOT EXISTS idx_file_paths_filename ON file_paths(filename);

CREATE TABLE IF NOT EXISTS blob_metadata (
    blob_id INT NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (blob_id, code)
);

CREATE TABLE IF NOT EXISTS blob_content (
    blob_id INT NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    code TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS index_queue (
    blob_id INT PRIMARY KEY REFERENCES blobs(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS errors (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    job_id INT REFERENCES jobs(id) ON DELETE CASCADE,
    blob_id INT REFERENCES blobs(id) ON DELETE CASCADE,
    processor TEXT,
    step SMALLINT NOT NULL,
    message TEXT NOT NULL
);
