package scanner

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/zeebo/blake3"
)

// FileStat holds the stat information for a file.
type FileStat struct {
	Path  string
	Mtime int64
	Size  int64
}

// CandidateType describes why a file is a candidate for syncing.
type CandidateType int

const (
	// CandidateNew indicates a file not in scan_state.
	CandidateNew CandidateType = iota
	// CandidateModified indicates a file whose mtime or size changed.
	CandidateModified
	// CandidatePendingRetry indicates a file computed but never ACKed.
	CandidatePendingRetry
)

// Candidate represents a file that needs to be sent to the server.
type Candidate struct {
	Type     CandidateType
	Path     string
	Mtime    int64
	Size     int64
	Blake3   string
	MimeType string
}

// ScanResult holds the results of a complete scan.
type ScanResult struct {
	TotalFiles int
	Candidates []Candidate
	Deleted    []string
}

// WalkDirectory performs Step 1: recursive stat pass.
// Returns a map of relative file paths to their FileStat.
func WalkDirectory(root string, verbose bool) (map[string]FileStat, error) {
	files := make(map[string]FileStat)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if verbose {
			fmt.Println(path)
		}
		if err != nil {
			return nil // skip files we can't stat
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		files[relPath] = FileStat{
			Path:  relPath,
			Mtime: info.ModTime().Unix(),
			Size:  info.Size(),
		}
		return nil
	})
	return files, err
}

// DetectChanges performs Step 2: compare current files against scan_state.
func DetectChanges(currentFiles map[string]FileStat, stateEntries map[string]*StateEntry) (candidates []Candidate, deleted []string) {
	// Check current files against state
	for path, stat := range currentFiles {
		entry, exists := stateEntries[path]
		if !exists {
			// New file
			candidates = append(candidates, Candidate{
				Type:  CandidateNew,
				Path:  path,
				Mtime: stat.Mtime,
				Size:  stat.Size,
			})
			continue
		}
		if entry.Mtime != stat.Mtime || entry.Size != stat.Size {
			// Modified file
			candidates = append(candidates, Candidate{
				Type:  CandidateModified,
				Path:  path,
				Mtime: stat.Mtime,
				Size:  stat.Size,
			})
			continue
		}
		// Same mtime and size
		if entry.SentAt != nil {
			// Already indexed, skip
			continue
		}
		// Pending retry: blake3 already computed but never ACKed
		blake3Val := ""
		if entry.Blake3 != nil {
			blake3Val = *entry.Blake3
		}
		candidates = append(candidates, Candidate{
			Type:   CandidatePendingRetry,
			Path:   path,
			Mtime:  stat.Mtime,
			Size:   stat.Size,
			Blake3: blake3Val,
		})
	}

	// Check for deleted files
	for path := range stateEntries {
		if _, exists := currentFiles[path]; !exists {
			deleted = append(deleted, path)
		}
	}

	return candidates, deleted
}

// ComputeBLAKE3 performs Step 3: streaming BLAKE3 hash computation.
func ComputeBLAKE3(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening file %s: %w", path, err)
	}
	defer f.Close()

	hasher := blake3.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", fmt.Errorf("hashing file %s: %w", path, err)
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// DetectMIME performs Step 4: detect MIME type from magic bytes.
func DetectMIME(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening file %s: %w", path, err)
	}
	defer f.Close()

	buf := make([]byte, 512)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("reading file %s: %w", path, err)
	}
	return http.DetectContentType(buf[:n]), nil
}

// ProcessCandidates performs Steps 3-4: compute BLAKE3 and detect MIME for candidates.
// root is the base directory to use for opening the files.
func ProcessCandidates(root string, candidates []Candidate) error {
	for i := range candidates {
		c := &candidates[i]
		absPath := filepath.Join(root, c.Path)
		// Step 3: BLAKE3 computation (skip for pending retries)
		if c.Type != CandidatePendingRetry {
			hash, err := ComputeBLAKE3(absPath)
			if err != nil {
				return err
			}
			c.Blake3 = hash
		}
		// Step 4: MIME detection
		mime, err := DetectMIME(absPath)
		if err != nil {
			return err
		}
		c.MimeType = mime
	}
	return nil
}
