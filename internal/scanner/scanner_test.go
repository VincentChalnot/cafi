package scanner

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/zeebo/blake3"
)

// ---------- DetectChanges tests ----------

func TestDetectChanges_NewFile(t *testing.T) {
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}
	state := map[string]*StateEntry{}

	candidates, deleted := DetectChanges(current, state)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Type != CandidateNew {
		t.Errorf("expected CandidateNew, got %d", candidates[0].Type)
	}
	if candidates[0].Path != "/tmp/a.txt" {
		t.Errorf("expected path /tmp/a.txt, got %s", candidates[0].Path)
	}
	if len(deleted) != 0 {
		t.Errorf("expected no deleted, got %d", len(deleted))
	}
}

func TestDetectChanges_ModifiedFile(t *testing.T) {
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 200, Size: 50},
	}
	state := map[string]*StateEntry{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}

	candidates, deleted := DetectChanges(current, state)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Type != CandidateModified {
		t.Errorf("expected CandidateModified, got %d", candidates[0].Type)
	}
	if len(deleted) != 0 {
		t.Errorf("expected no deleted, got %d", len(deleted))
	}
}

func TestDetectChanges_ModifiedFileSize(t *testing.T) {
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 999},
	}
	state := map[string]*StateEntry{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}

	candidates, _ := DetectChanges(current, state)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Type != CandidateModified {
		t.Errorf("expected CandidateModified, got %d", candidates[0].Type)
	}
}

func TestDetectChanges_UnchangedAndSent(t *testing.T) {
	sentAt := int64(300)
	hash := "abc123"
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}
	state := map[string]*StateEntry{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50, Blake3: &hash, SentAt: &sentAt},
	}

	candidates, deleted := DetectChanges(current, state)

	if len(candidates) != 0 {
		t.Errorf("expected 0 candidates (file already sent), got %d", len(candidates))
	}
	if len(deleted) != 0 {
		t.Errorf("expected no deleted, got %d", len(deleted))
	}
}

func TestDetectChanges_PendingRetry(t *testing.T) {
	hash := "abc123"
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}
	state := map[string]*StateEntry{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50, Blake3: &hash, SentAt: nil},
	}

	candidates, deleted := DetectChanges(current, state)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Type != CandidatePendingRetry {
		t.Errorf("expected CandidatePendingRetry, got %d", candidates[0].Type)
	}
	if candidates[0].Blake3 != hash {
		t.Errorf("expected blake3 %q, got %q", hash, candidates[0].Blake3)
	}
	if len(deleted) != 0 {
		t.Errorf("expected no deleted, got %d", len(deleted))
	}
}

func TestDetectChanges_PendingRetryNilBlake3(t *testing.T) {
	current := map[string]FileStat{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50},
	}
	state := map[string]*StateEntry{
		"/tmp/a.txt": {Path: "/tmp/a.txt", Mtime: 100, Size: 50, Blake3: nil, SentAt: nil},
	}

	candidates, _ := DetectChanges(current, state)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Type != CandidatePendingRetry {
		t.Errorf("expected CandidatePendingRetry, got %d", candidates[0].Type)
	}
	if candidates[0].Blake3 != "" {
		t.Errorf("expected empty blake3 for nil entry, got %q", candidates[0].Blake3)
	}
}

func TestDetectChanges_DeletedFile(t *testing.T) {
	current := map[string]FileStat{}
	state := map[string]*StateEntry{
		"/tmp/gone.txt": {Path: "/tmp/gone.txt", Mtime: 100, Size: 50},
	}

	candidates, deleted := DetectChanges(current, state)

	if len(candidates) != 0 {
		t.Errorf("expected 0 candidates, got %d", len(candidates))
	}
	if len(deleted) != 1 {
		t.Fatalf("expected 1 deleted, got %d", len(deleted))
	}
	if deleted[0] != "/tmp/gone.txt" {
		t.Errorf("expected /tmp/gone.txt in deleted, got %s", deleted[0])
	}
}

func TestDetectChanges_MixedScenarios(t *testing.T) {
	sentAt := int64(300)
	hash := "existinghash"
	retryHash := "retryhash"

	current := map[string]FileStat{
		"/tmp/new.txt":       {Path: "/tmp/new.txt", Mtime: 100, Size: 10},
		"/tmp/modified.txt":  {Path: "/tmp/modified.txt", Mtime: 200, Size: 20},
		"/tmp/unchanged.txt": {Path: "/tmp/unchanged.txt", Mtime: 100, Size: 50},
		"/tmp/retry.txt":     {Path: "/tmp/retry.txt", Mtime: 100, Size: 30},
	}
	state := map[string]*StateEntry{
		"/tmp/modified.txt":  {Path: "/tmp/modified.txt", Mtime: 100, Size: 20, Blake3: &hash},
		"/tmp/unchanged.txt": {Path: "/tmp/unchanged.txt", Mtime: 100, Size: 50, Blake3: &hash, SentAt: &sentAt},
		"/tmp/retry.txt":     {Path: "/tmp/retry.txt", Mtime: 100, Size: 30, Blake3: &retryHash, SentAt: nil},
		"/tmp/deleted.txt":   {Path: "/tmp/deleted.txt", Mtime: 100, Size: 40},
	}

	candidates, deleted := DetectChanges(current, state)

	// Build lookup maps for deterministic assertions
	byType := make(map[CandidateType][]Candidate)
	for _, c := range candidates {
		byType[c.Type] = append(byType[c.Type], c)
	}

	if len(byType[CandidateNew]) != 1 {
		t.Errorf("expected 1 new, got %d", len(byType[CandidateNew]))
	}
	if len(byType[CandidateModified]) != 1 {
		t.Errorf("expected 1 modified, got %d", len(byType[CandidateModified]))
	}
	if len(byType[CandidatePendingRetry]) != 1 {
		t.Errorf("expected 1 pending retry, got %d", len(byType[CandidatePendingRetry]))
	}
	if len(candidates) != 3 {
		t.Errorf("expected 3 total candidates (unchanged skipped), got %d", len(candidates))
	}

	sort.Strings(deleted)
	if len(deleted) != 1 || deleted[0] != "/tmp/deleted.txt" {
		t.Errorf("expected deleted=[/tmp/deleted.txt], got %v", deleted)
	}
}

// ---------- ComputeBLAKE3 tests ----------

func TestComputeBLAKE3_KnownContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hello.txt")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ComputeBLAKE3(path)
	if err != nil {
		t.Fatalf("ComputeBLAKE3 error: %v", err)
	}

	// Compute expected hash using the library directly
	h := blake3.New()
	h.Write(content)
	expected := fmt.Sprintf("%x", h.Sum(nil))

	if got != expected {
		t.Errorf("hash mismatch: got %s, expected %s", got, expected)
	}
}

func TestComputeBLAKE3_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ComputeBLAKE3(path)
	if err != nil {
		t.Fatalf("ComputeBLAKE3 error: %v", err)
	}

	h := blake3.New()
	expected := fmt.Sprintf("%x", h.Sum(nil))

	if got != expected {
		t.Errorf("empty file hash mismatch: got %s, expected %s", got, expected)
	}
}

func TestComputeBLAKE3_LargerFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.bin")

	// Write 1MB of repeated content
	chunk := []byte("abcdefghijklmnopqrstuvwxyz0123456789\n")
	var content []byte
	for len(content) < 1<<20 {
		content = append(content, chunk...)
	}
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := ComputeBLAKE3(path)
	if err != nil {
		t.Fatalf("ComputeBLAKE3 error: %v", err)
	}

	h := blake3.New()
	h.Write(content)
	expected := fmt.Sprintf("%x", h.Sum(nil))

	if got != expected {
		t.Errorf("large file hash mismatch: got %s, expected %s", got, expected)
	}

	// Sanity: hash should be a non-empty hex string
	if len(got) == 0 {
		t.Error("hash should not be empty")
	}
}

func TestComputeBLAKE3_NonexistentFile(t *testing.T) {
	_, err := ComputeBLAKE3("/nonexistent/path/file.txt")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

// ---------- DetectMIME tests ----------

func TestDetectMIME_PDF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pdf")
	// PDF magic bytes
	pdfContent := []byte("%PDF-1.4 fake pdf content here")
	if err := os.WriteFile(path, pdfContent, 0644); err != nil {
		t.Fatal(err)
	}

	mime, err := DetectMIME(path)
	if err != nil {
		t.Fatalf("DetectMIME error: %v", err)
	}

	if mime != "application/pdf" {
		t.Errorf("expected application/pdf, got %s", mime)
	}
}

func TestDetectMIME_PlainText(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, []byte("just some plain text content\n"), 0644); err != nil {
		t.Fatal(err)
	}

	mime, err := DetectMIME(path)
	if err != nil {
		t.Fatalf("DetectMIME error: %v", err)
	}

	if !strings.HasPrefix(mime, "text/plain") {
		t.Errorf("expected text/plain*, got %s", mime)
	}
}

func TestDetectMIME_NonexistentFile(t *testing.T) {
	_, err := DetectMIME("/nonexistent/path/file.txt")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}
