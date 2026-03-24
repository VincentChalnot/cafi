package db

import (
	"context"
	"embed"
	"fmt"

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

// --- Users ---

// CreateUser inserts a new user row and returns the generated ID.
func (d *DB) CreateUser(ctx context.Context, username string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`INSERT INTO users (username) VALUES ($1) RETURNING id`, username).Scan(&id)
	return id, err
}

// DeleteUser removes a user by username.
func (d *DB) DeleteUser(ctx context.Context, username string) error {
	ct, err := d.Pool.Exec(ctx, `DELETE FROM users WHERE username = $1`, username)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

// GetUserID returns the user ID for a given username.
func (d *DB) GetUserID(ctx context.Context, username string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx, `SELECT id FROM users WHERE username = $1`, username).Scan(&id)
	return id, err
}

// ListUsers returns all usernames.
func (d *DB) ListUsers(ctx context.Context) ([]string, error) {
	rows, err := d.Pool.Query(ctx, `SELECT username FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// --- Sources ---

// CreateSource inserts a new source and returns the generated ID.
func (d *DB) CreateSource(ctx context.Context, userID int, code string, strategy int, path *string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`INSERT INTO sources (user_id, code, strategy, path) VALUES ($1, $2, $3, $4) RETURNING id`,
		userID, code, strategy, path).Scan(&id)
	return id, err
}

// DeleteSource removes a source by user_id and code.
func (d *DB) DeleteSource(ctx context.Context, userID int, code string) error {
	ct, err := d.Pool.Exec(ctx,
		`DELETE FROM sources WHERE user_id = $1 AND code = $2`, userID, code)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("source %q not found for user %d", code, userID)
	}
	return nil
}

// UpdateSource updates strategy and/or path on a source.
func (d *DB) UpdateSource(ctx context.Context, userID int, code string, strategy int, path *string) error {
	ct, err := d.Pool.Exec(ctx,
		`UPDATE sources SET strategy = $1, path = $2 WHERE user_id = $3 AND code = $4`,
		strategy, path, userID, code)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("source %q not found for user %d", code, userID)
	}
	return nil
}

// GetSourceID returns the source ID for a user_id and code.
func (d *DB) GetSourceID(ctx context.Context, userID int, code string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`SELECT id FROM sources WHERE user_id = $1 AND code = $2`, userID, code).Scan(&id)
	return id, err
}

// SourceInfo holds basic information about a source.
type SourceInfo struct {
	ID       int
	UserID   int
	Username string
	Code     string
	Strategy int
	Path     *string
}

// ListSources returns all sources.
func (d *DB) ListSources(ctx context.Context) ([]SourceInfo, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT s.id, s.user_id, u.username, s.code, s.strategy, s.path
		 FROM sources s
		 JOIN users u ON s.user_id = u.id
		 ORDER BY s.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []SourceInfo
	for rows.Next() {
		var s SourceInfo
		if err := rows.Scan(&s.ID, &s.UserID, &s.Username, &s.Code, &s.Strategy, &s.Path); err != nil {
			return nil, err
		}
		sources = append(sources, s)
	}
	return sources, rows.Err()
}

// --- Tokens ---

// CreateToken inserts a token and returns the generated ID.
func (d *DB) CreateToken(ctx context.Context, userID int, name, tokenHash string, expireAt string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`INSERT INTO tokens (user_id, name, token_hash, expire_at)
		 VALUES ($1, $2, $3, $4::timestamptz) RETURNING id`,
		userID, name, tokenHash, expireAt).Scan(&id)
	return id, err
}

// DeleteToken removes a token by user_id and name.
func (d *DB) DeleteToken(ctx context.Context, userID int, name string) error {
	ct, err := d.Pool.Exec(ctx,
		`DELETE FROM tokens WHERE user_id = $1 AND name = $2`, userID, name)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("token %q not found for user %d", name, userID)
	}
	return nil
}

// RefreshToken atomically replaces the token hash for a token.
func (d *DB) RefreshToken(ctx context.Context, userID int, name, tokenHash string, expireAt string) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ct, err := tx.Exec(ctx,
		`UPDATE tokens SET token_hash = $1, expire_at = $2::timestamptz WHERE user_id = $3 AND name = $4`,
		tokenHash, expireAt, userID, name)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("token %q not found for user %d", name, userID)
	}
	return tx.Commit(ctx)
}

// GetTokenID returns the token ID for a user_id and name.
func (d *DB) GetTokenID(ctx context.Context, userID int, name string) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`SELECT id FROM tokens WHERE user_id = $1 AND name = $2`, userID, name).Scan(&id)
	return id, err
}

// AddTokenSources adds source_id entries to token_sources.
func (d *DB) AddTokenSources(ctx context.Context, tokenID int, sourceIDs []int) error {
	for _, sid := range sourceIDs {
		_, err := d.Pool.Exec(ctx,
			`INSERT INTO token_sources (token_id, source_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			tokenID, sid)
		if err != nil {
			return err
		}
	}
	return nil
}

// RemoveTokenSources removes source_id entries from token_sources.
func (d *DB) RemoveTokenSources(ctx context.Context, tokenID int, sourceIDs []int) error {
	for _, sid := range sourceIDs {
		_, err := d.Pool.Exec(ctx,
			`DELETE FROM token_sources WHERE token_id = $1 AND source_id = $2`,
			tokenID, sid)
		if err != nil {
			return err
		}
	}
	return nil
}

// --- Auth token resolution ---

// TokenInfo holds the token hash, user_id, and linked source IDs for auth.
type TokenInfo struct {
	ID        int
	UserID    int
	Username  string
	Name      string
	TokenHash string
	ExpireAt  string
	SourceIDs []int
}

// GetAllTokens retrieves all tokens.
func (d *DB) GetAllTokens(ctx context.Context) ([]TokenInfo, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT t.id, t.user_id, u.username, t.name, t.token_hash, t.expire_at
		 FROM tokens t
		 JOIN users u ON t.user_id = u.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []TokenInfo
	for rows.Next() {
		var ti TokenInfo
		var expireAt any
		if err := rows.Scan(&ti.ID, &ti.UserID, &ti.Username, &ti.Name, &ti.TokenHash, &expireAt); err != nil {
			return nil, err
		}
		ti.ExpireAt = fmt.Sprintf("%v", expireAt)
		tokens = append(tokens, ti)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Load source_ids for each token
	for i := range tokens {
		srcRows, err := d.Pool.Query(ctx,
			`SELECT source_id FROM token_sources WHERE token_id = $1`, tokens[i].ID)
		if err != nil {
			return nil, err
		}
		for srcRows.Next() {
			var sid int
			if err := srcRows.Scan(&sid); err != nil {
				srcRows.Close()
				return nil, err
			}
			tokens[i].SourceIDs = append(tokens[i].SourceIDs, sid)
		}
		srcRows.Close()
		if err := srcRows.Err(); err != nil {
			return nil, err
		}
	}
	return tokens, nil
}

// GetSourceCodeToID returns a map of source code to source ID for the given source IDs.
func (d *DB) GetSourceCodeToID(ctx context.Context, sourceIDs []int) (map[string]int, error) {
	result := make(map[string]int)
	for _, sid := range sourceIDs {
		var code string
		err := d.Pool.QueryRow(ctx, `SELECT code FROM sources WHERE id = $1`, sid).Scan(&code)
		if err != nil {
			return nil, err
		}
		result[code] = sid
	}
	return result, nil
}

// --- Blobs ---

// UpsertBlob inserts a blob row if it does not exist and returns its ID.
func (d *DB) UpsertBlob(ctx context.Context, blake3, mimeType string, size int64) (int, error) {
	var id int
	err := d.Pool.QueryRow(ctx,
		`INSERT INTO blobs (blake3, mime_type, size)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (blake3) DO UPDATE SET blake3 = EXCLUDED.blake3
		 RETURNING id`,
		blake3, mimeType, size).Scan(&id)
	return id, err
}

// --- File Paths ---

// UpsertFilePath inserts or updates a file path entry.
func (d *DB) UpsertFilePath(ctx context.Context, sourceID int, folder, filename string, blobID int, mtime int64) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO file_paths (source_id, folder, filename, blob_id, mtime, last_seen_at, deleted_at)
		 VALUES ($1, $2, $3, $4, $5, now(), NULL)
		 ON CONFLICT (source_id, folder, filename) DO UPDATE SET
		    blob_id = EXCLUDED.blob_id,
		    mtime = EXCLUDED.mtime,
		    last_seen_at = now(),
		    deleted_at = NULL`,
		sourceID, folder, filename, blobID, mtime)
	return err
}

// MarkFileDeleted sets deleted_at on a file path entry.
func (d *DB) MarkFileDeleted(ctx context.Context, sourceID int, folder, filename string) error {
	_, err := d.Pool.Exec(ctx,
		`UPDATE file_paths SET deleted_at = now() WHERE source_id = $1 AND folder = $2 AND filename = $3`,
		sourceID, folder, filename)
	return err
}

// FilePathRow represents a row from file_paths joined with blobs.
type FilePathRow struct {
	SourceID int
	Folder   string
	Filename string
	Blake3   string
	MimeType *string
	Size     int64
	Mtime    int64
}

// QueryFilePaths queries file_paths joined with blobs. Supports optional source ID list and LIKE filter.
func (d *DB) QueryFilePaths(ctx context.Context, sourceIDs []int, likePattern string) ([]FilePathRow, error) {
	query := `SELECT fp.source_id, fp.folder, fp.filename, b.blake3, b.mime_type, b.size, fp.mtime
	          FROM file_paths fp
	          JOIN blobs b ON fp.blob_id = b.id
	          WHERE fp.deleted_at IS NULL`
	var args []any
	argIdx := 1

	if len(sourceIDs) > 0 {
		query += fmt.Sprintf(" AND fp.source_id = ANY($%d)", argIdx)
		args = append(args, sourceIDs)
		argIdx++
	}
	if likePattern != "" {
		query += fmt.Sprintf(" AND (fp.folder || '/' || fp.filename) LIKE $%d", argIdx)
		args = append(args, likePattern)
		argIdx++
	}
	query += " ORDER BY fp.source_id, fp.folder, fp.filename"

	rows, err := d.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []FilePathRow
	for rows.Next() {
		var r FilePathRow
		if err := rows.Scan(&r.SourceID, &r.Folder, &r.Filename, &r.Blake3, &r.MimeType, &r.Size, &r.Mtime); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// DeleteFilePathsBySource removes all file_paths for a specific source.
func (d *DB) DeleteFilePathsBySource(ctx context.Context, sourceID int) error {
	_, err := d.Pool.Exec(ctx, `DELETE FROM file_paths WHERE source_id = $1`, sourceID)
	return err
}

// DeleteSourceByID removes a source by its integer ID.
func (d *DB) DeleteSourceByID(ctx context.Context, sourceID int) error {
	_, err := d.Pool.Exec(ctx, `DELETE FROM sources WHERE id = $1`, sourceID)
	return err
}
