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
	SourceID string
	Path     string
	Blake3   *string
	Mtime    int64
	Size     int64
	SentAt   *int64
}

// OpenStateDB opens or creates the SQLite state database.
func OpenStateDB(path string) (*StateDB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("opening state db: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS scan_state (
		source_id TEXT NOT NULL DEFAULT '',
		path TEXT NOT NULL,
		blake3 TEXT,
		mtime INTEGER,
		size INTEGER,
		sent_at INTEGER,
		PRIMARY KEY (source_id, path)
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating scan_state table: %w", err)
	}
	// Migration: Add source_id and update PK if it doesn't exist (for existing DBs)
	// SQLite doesn't support changing PK via ALTER TABLE, so we recreate if needed.
	var hasSourceId bool
	var pkColumns int
	rows, err := db.Query("PRAGMA table_info(scan_state)")
	if err == nil {
		for rows.Next() {
			var cid int
			var name, dtype string
			var notnull int
			var dflt_value interface{}
			var pk int
			if err := rows.Scan(&cid, &name, &dtype, &notnull, &dflt_value, &pk); err == nil {
				if name == "source_id" {
					hasSourceId = true
				}
				if pk > 0 {
					pkColumns++
				}
			}
		}
		rows.Close()
	}
	if !hasSourceId || pkColumns != 2 {
		// Proper migration: create new table, copy data, drop old, rename new.
		// For simplicity, if it's a small local state, we can just recreate it.
		// But let's try to be a bit more gentle.
		_, _ = db.Exec("ALTER TABLE scan_state RENAME TO scan_state_old")
		if _, err := db.Exec(`CREATE TABLE scan_state (
			source_id TEXT NOT NULL DEFAULT '',
			path TEXT NOT NULL,
			blake3 TEXT,
			mtime INTEGER,
			size INTEGER,
			sent_at INTEGER,
			PRIMARY KEY (source_id, path)
		)`); err == nil {
			// Copy data from old table.
			if hasSourceId {
				_, _ = db.Exec("INSERT INTO scan_state (source_id, path, blake3, mtime, size, sent_at) SELECT source_id, path, blake3, mtime, size, sent_at FROM scan_state_old")
			} else {
				_, _ = db.Exec("INSERT INTO scan_state (source_id, path, blake3, mtime, size, sent_at) SELECT '', path, blake3, mtime, size, sent_at FROM scan_state_old")
			}
			_, _ = db.Exec("DROP TABLE scan_state_old")
		} else {
			// If creation failed, rename back.
			_, _ = db.Exec("ALTER TABLE scan_state_old RENAME TO scan_state")
		}
	}
	return &StateDB{db: db}, nil
}

// Close closes the state database.
func (s *StateDB) Close() error {
	return s.db.Close()
}

// Get retrieves a state entry by source_id and path. Returns nil if not found.
func (s *StateDB) Get(sourceID, path string) (*StateEntry, error) {
	var e StateEntry
	err := s.db.QueryRow(
		`SELECT source_id, path, blake3, mtime, size, sent_at FROM scan_state WHERE source_id = ? AND path = ?`, sourceID, path,
	).Scan(&e.SourceID, &e.Path, &e.Blake3, &e.Mtime, &e.Size, &e.SentAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// GetAll retrieves all state entries for a given source_id.
func (s *StateDB) GetAll(sourceID string) (map[string]*StateEntry, error) {
	rows, err := s.db.Query(`SELECT source_id, path, blake3, mtime, size, sent_at FROM scan_state WHERE source_id = ?`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]*StateEntry)
	for rows.Next() {
		var e StateEntry
		if err := rows.Scan(&e.SourceID, &e.Path, &e.Blake3, &e.Mtime, &e.Size, &e.SentAt); err != nil {
			return nil, err
		}
		result[e.Path] = &e
	}
	return result, rows.Err()
}

// Upsert inserts or updates a state entry (for new/modified candidates after BLAKE3 computation).
func (s *StateDB) Upsert(sourceID, path, blake3 string, mtime, size int64) error {
	_, err := s.db.Exec(
		`INSERT INTO scan_state (source_id, path, blake3, mtime, size, sent_at)
		 VALUES (?, ?, ?, ?, ?, NULL)
		 ON CONFLICT(source_id, path) DO UPDATE SET blake3 = ?, mtime = ?, size = ?, sent_at = NULL`,
		sourceID, path, blake3, mtime, size, blake3, mtime, size)
	return err
}

// MarkSent updates sent_at for a given source_id and path.
func (s *StateDB) MarkSent(sourceID, path string) error {
	_, err := s.db.Exec(
		`UPDATE scan_state SET sent_at = ? WHERE source_id = ? AND path = ?`,
		time.Now().Unix(), sourceID, path)
	return err
}

// Delete removes a state entry by source_id and path.
func (s *StateDB) Delete(sourceID, path string) error {
	_, err := s.db.Exec(`DELETE FROM scan_state WHERE source_id = ? AND path = ?`, sourceID, path)
	return err
}
