package db

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// DB wraps a pgx connection pool and provides database operations.
type DB struct {
	Pool *pgxpool.Pool
}

// New creates a new DB instance from an existing pool.
func New(pool *pgxpool.Pool) *DB {
	return &DB{Pool: pool}
}

// RunMigrations reads and executes all embedded SQL migration files.
func (d *DB) RunMigrations(ctx context.Context) error {
	data, err := migrationsFS.ReadFile("migrations/001_initial.sql")
	if err != nil {
		return fmt.Errorf("reading migration: %w", err)
	}
	_, err = d.Pool.Exec(ctx, string(data))
	if err != nil {
		return fmt.Errorf("running migration: %w", err)
	}
	return nil
}

// UpsertUser ensures a user row exists.
func (d *DB) UpsertUser(ctx context.Context, id string) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, id)
	return err
}

// UpsertSource ensures a source row exists, updating the token hash if needed.
func (d *DB) UpsertSource(ctx context.Context, id, userID, tokenHash string) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO sources (id, user_id, token_hash)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET token_hash = EXCLUDED.token_hash`,
		id, userID, tokenHash)
	return err
}

// UpsertBlob inserts a blob row if it does not exist.
func (d *DB) UpsertBlob(ctx context.Context, blake3, mimeType string, size int64) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO blobs (blake3, mime_type, size)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (blake3) DO NOTHING`,
		blake3, mimeType, size)
	return err
}

// UpsertFilePath inserts or updates a file path entry.
func (d *DB) UpsertFilePath(ctx context.Context, sourceID, path, blake3 string, mtime int64) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO file_paths (source_id, path, blake3, mtime, last_seen_at, deleted_at)
		 VALUES ($1, $2, $3, $4, now(), NULL)
		 ON CONFLICT (source_id, path) DO UPDATE SET
		    blake3 = EXCLUDED.blake3,
		    mtime = EXCLUDED.mtime,
		    last_seen_at = now(),
		    deleted_at = NULL`,
		sourceID, path, blake3, mtime)
	return err
}

// MarkFileDeleted sets deleted_at on a file path entry.
func (d *DB) MarkFileDeleted(ctx context.Context, sourceID, path string) error {
	_, err := d.Pool.Exec(ctx,
		`UPDATE file_paths SET deleted_at = now() WHERE source_id = $1 AND path = $2`,
		sourceID, path)
	return err
}

// SourceToken holds a source ID and its bcrypt token hash.
type SourceToken struct {
	SourceID  string
	TokenHash string
}

// GetAllSourceTokens retrieves all source IDs and their token hashes.
func (d *DB) GetAllSourceTokens(ctx context.Context) ([]SourceToken, error) {
	rows, err := d.Pool.Query(ctx, `SELECT id, token_hash FROM sources`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tokens []SourceToken
	for rows.Next() {
		var st SourceToken
		if err := rows.Scan(&st.SourceID, &st.TokenHash); err != nil {
			return nil, err
		}
		tokens = append(tokens, st)
	}
	return tokens, rows.Err()
}

// FilePathRow represents a row from file_paths joined with blobs.
type FilePathRow struct {
	SourceID string
	Path     string
	Blake3   string
	MimeType *string
	Size     int64
	Mtime    int64
	LastSeen time.Time
}

// QueryFilePaths queries file_paths joined with blobs. Supports optional source and LIKE pattern filters.
func (d *DB) QueryFilePaths(ctx context.Context, sourceID, likePattern string) ([]FilePathRow, error) {
	query := `SELECT fp.source_id, fp.path, fp.blake3, b.mime_type, b.size, fp.mtime, fp.last_seen_at
	          FROM file_paths fp
	          JOIN blobs b ON fp.blake3 = b.blake3
	          WHERE fp.deleted_at IS NULL`
	var args []any
	argIdx := 1

	if sourceID != "" {
		query += fmt.Sprintf(" AND fp.source_id = $%d", argIdx)
		args = append(args, sourceID)
		argIdx++
	}
	if likePattern != "" {
		query += fmt.Sprintf(" AND fp.path LIKE $%d", argIdx)
		args = append(args, likePattern)
		argIdx++
	}
	query += " ORDER BY fp.source_id, fp.path"

	rows, err := d.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []FilePathRow
	for rows.Next() {
		var r FilePathRow
		if err := rows.Scan(&r.SourceID, &r.Path, &r.Blake3, &r.MimeType, &r.Size, &r.Mtime, &r.LastSeen); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
