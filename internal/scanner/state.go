package scanner

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// StateDB manages the local SQLite scan state.
type StateDB struct {
	db *sql.DB
}

// StateEntry represents a row in the scan_state table.
type StateEntry struct {
	Path   string
	Blake3 *string
	Mtime  int64
	Size   int64
	SentAt *int64
}

// OpenStateDB opens or creates the SQLite state database.
func OpenStateDB(path string) (*StateDB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("opening state db: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS scan_state (
		path TEXT PRIMARY KEY,
		blake3 TEXT,
		mtime INTEGER,
		size INTEGER,
		sent_at INTEGER
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating scan_state table: %w", err)
	}
	return &StateDB{db: db}, nil
}

// Close closes the state database.
func (s *StateDB) Close() error {
	return s.db.Close()
}

// Get retrieves a state entry by path. Returns nil if not found.
func (s *StateDB) Get(path string) (*StateEntry, error) {
	var e StateEntry
	err := s.db.QueryRow(
		`SELECT path, blake3, mtime, size, sent_at FROM scan_state WHERE path = ?`, path,
	).Scan(&e.Path, &e.Blake3, &e.Mtime, &e.Size, &e.SentAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// GetAll retrieves all state entries.
func (s *StateDB) GetAll() (map[string]*StateEntry, error) {
	rows, err := s.db.Query(`SELECT path, blake3, mtime, size, sent_at FROM scan_state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]*StateEntry)
	for rows.Next() {
		var e StateEntry
		if err := rows.Scan(&e.Path, &e.Blake3, &e.Mtime, &e.Size, &e.SentAt); err != nil {
			return nil, err
		}
		result[e.Path] = &e
	}
	return result, rows.Err()
}

// Upsert inserts or updates a state entry (for new/modified candidates after BLAKE3 computation).
func (s *StateDB) Upsert(path, blake3 string, mtime, size int64) error {
	_, err := s.db.Exec(
		`INSERT INTO scan_state (path, blake3, mtime, size, sent_at)
		 VALUES (?, ?, ?, ?, NULL)
		 ON CONFLICT(path) DO UPDATE SET blake3 = ?, mtime = ?, size = ?, sent_at = NULL`,
		path, blake3, mtime, size, blake3, mtime, size)
	return err
}

// MarkSent updates sent_at for a given path.
func (s *StateDB) MarkSent(path string) error {
	_, err := s.db.Exec(
		`UPDATE scan_state SET sent_at = ? WHERE path = ?`,
		time.Now().Unix(), path)
	return err
}

// Delete removes a state entry by path.
func (s *StateDB) Delete(path string) error {
	_, err := s.db.Exec(`DELETE FROM scan_state WHERE path = ?`, path)
	return err
}
