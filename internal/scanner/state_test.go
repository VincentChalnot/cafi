package scanner

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestStateDB_MultiSource(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_state.db")

	db, err := OpenStateDB(dbPath)
	if err != nil {
		t.Fatalf("failed to open state db: %v", err)
	}
	defer db.Close()

	source1 := "source1"
	source2 := "source2"
	path := "file.txt"
	hash1 := "hash1"
	hash2 := "hash2"

	// Upsert for source1
	if err := db.Upsert(source1, path, hash1, 100, 50); err != nil {
		t.Fatalf("upsert source1 failed: %v", err)
	}

	// Upsert for source2 (same path)
	if err := db.Upsert(source2, path, hash2, 200, 60); err != nil {
		t.Fatalf("upsert source2 failed: %v", err)
	}

	// Verify both exist and are distinct
	e1, err := db.Get(source1, path)
	if err != nil || e1 == nil {
		t.Fatalf("get source1 failed: %v", err)
	}
	if *e1.Blake3 != hash1 {
		t.Errorf("expected %s, got %s", hash1, *e1.Blake3)
	}

	e2, err := db.Get(source2, path)
	if err != nil || e2 == nil {
		t.Fatalf("get source2 failed: %v", err)
	}
	if *e2.Blake3 != hash2 {
		t.Errorf("expected %s, got %s", hash2, *e2.Blake3)
	}

	// GetAll for source1 should only return source1's entry
	all1, err := db.GetAll(source1)
	if err != nil {
		t.Fatalf("getAll source1 failed: %v", err)
	}
	if len(all1) != 1 {
		t.Errorf("expected 1 entry, got %d", len(all1))
	}
	if all1[path].Blake3 == nil || *all1[path].Blake3 != hash1 {
		t.Errorf("wrong entry in getAll source1")
	}

	// MarkSent for source1
	if err := db.MarkSent(source1, path); err != nil {
		t.Fatalf("markSent source1 failed: %v", err)
	}

	e1, _ = db.Get(source1, path)
	if e1.SentAt == nil {
		t.Errorf("expected sent_at to be set for source1")
	}
	e2, _ = db.Get(source2, path)
	if e2.SentAt != nil {
		t.Errorf("expected sent_at to be nil for source2")
	}

	// Delete source1
	if err := db.Delete(source1, path); err != nil {
		t.Fatalf("delete source1 failed: %v", err)
	}

	e1, _ = db.Get(source1, path)
	if e1 != nil {
		t.Errorf("expected source1 entry to be deleted")
	}
	e2, _ = db.Get(source2, path)
	if e2 == nil {
		t.Errorf("expected source2 entry to still exist")
	}
}

func TestStateDB_Migration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "migration_test.db")

	// 1. Create old schema DB manually
	func() {
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		_, err = db.Exec(`CREATE TABLE scan_state (
			path TEXT PRIMARY KEY,
			blake3 TEXT,
			mtime INTEGER,
			size INTEGER,
			sent_at INTEGER
		)`)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`INSERT INTO scan_state (path, blake3, mtime, size) VALUES (?, ?, ?, ?)`, "old_path.txt", "old_hash", 123, 456)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// 2. Open with OpenStateDB, which should trigger migration
	stateDB, err := OpenStateDB(dbPath)
	if err != nil {
		t.Fatalf("OpenStateDB failed: %v", err)
	}
	defer stateDB.Close()

	// 3. Verify data is preserved and schema is correct
	entry, err := stateDB.Get("", "old_path.txt") // source_id should be empty for migrated rows
	if err != nil {
		t.Fatalf("failed to get migrated entry: %v", err)
	}
	if entry == nil {
		t.Fatal("migrated entry not found")
	}
	if entry.Blake3 == nil || *entry.Blake3 != "old_hash" {
		t.Errorf("expected old_hash, got %v", entry.Blake3)
	}

	// 4. Verify we can insert with source_id now
	err = stateDB.Upsert("new_source", "new_path.txt", "new_hash", 789, 101112)
	if err != nil {
		t.Fatalf("Upsert failed after migration: %v", err)
	}

	entry, _ = stateDB.Get("new_source", "new_path.txt")
	if entry == nil || entry.Blake3 == nil || *entry.Blake3 != "new_hash" {
		t.Fatal("failed to get entry inserted after migration")
	}
}
