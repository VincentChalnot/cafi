package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	cafiv1 "github.com/VincentChalnot/cafi/internal/proto/cafi/v1"
	"github.com/VincentChalnot/cafi/internal/scanner"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "cafi-client",
		Short: "Cafi content-addressable file indexer client",
	}

	var (
		serverAddr  string
		sourceID    string
		statePath   string
		parallelism int
		dryRun      bool
		resetState  bool
	)

	scanCmd := &cobra.Command{
		Use:   "scan [directory]",
		Short: "Scan a directory and sync file events to the server",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runScan(args[0], serverAddr, sourceID, statePath, parallelism, dryRun, resetState)
		},
	}

	defaultState := filepath.Join(os.Getenv("HOME"), ".cafi", "scan_state.db")
	scanCmd.Flags().StringVar(&serverAddr, "server", "localhost:50051", "gRPC server address")
	scanCmd.Flags().StringVar(&sourceID, "source", "", "Source identifier")
	scanCmd.Flags().StringVar(&statePath, "state", defaultState, "Path to SQLite state database")
	scanCmd.Flags().IntVar(&parallelism, "parallelism", 4, "Number of concurrent event sends")
	scanCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Perform scan only; do not connect to server or modify state")
	scanCmd.Flags().BoolVar(&resetState, "reset-state", false, "Delete state database before scanning")

	rootCmd.AddCommand(scanCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runScan(targetDir, serverAddr, sourceID, statePath string, parallelism int, dryRun, resetState bool) error {
	token := os.Getenv("CAFI_TOKEN")
	if token == "" && !dryRun {
		return fmt.Errorf("CAFI_TOKEN environment variable is required")
	}

	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("resolving absolute path for target dir: %w", err)
	}
	targetDir = absTargetDir

	start := time.Now()

	// Reset state if requested.
	if resetState {
		if err := os.Remove(statePath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing state db: %w", err)
		}
	}

	// Ensure the parent directory exists for the state database.
	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		return fmt.Errorf("creating state directory: %w", err)
	}

	stateDB, err := scanner.OpenStateDB(statePath)
	if err != nil {
		return fmt.Errorf("opening state db: %w", err)
	}
	defer stateDB.Close()

	// Step 1: Recursive stat pass.
	fmt.Println("Step 1: Walking directory...")
	currentFiles, err := scanner.WalkDirectory(targetDir)
	if err != nil {
		return fmt.Errorf("walking directory: %w", err)
	}
	fmt.Printf("  Found %d files\n", len(currentFiles))

	// Step 2: Detect changes against existing state.
	fmt.Println("Step 2: Detecting changes...")
	stateEntries, err := stateDB.GetAll(sourceID)
	if err != nil {
		return fmt.Errorf("reading state: %w", err)
	}
	candidates, deleted := scanner.DetectChanges(currentFiles, stateEntries)

	// Steps 3-4: Compute BLAKE3 hashes and detect MIME types.
	fmt.Println("Steps 3-4: Computing BLAKE3 hashes and detecting MIME types...")
	if err := scanner.ProcessCandidates(targetDir, candidates); err != nil {
		return fmt.Errorf("processing candidates: %w", err)
	}

	newCount, modCount, retryCount := countByType(candidates)

	// Dry-run: print what would be sent and exit without modifying state.
	if dryRun {
		fmt.Println("\n--- DRY RUN ---")
		for _, c := range candidates {
			fmt.Printf("  UPSERT %s (blake3=%s, mime=%s, size=%d)\n",
				c.Path, c.Blake3, c.MimeType, c.Size)
		}
		for _, p := range deleted {
			fmt.Printf("  DELETED %s\n", p)
		}
		printSummary(len(currentFiles), len(candidates), newCount, modCount, retryCount, len(deleted), 0, 0, start)
		return nil
	}

	// Persist computed hashes for new and modified candidates.
	for _, c := range candidates {
		if c.Type != scanner.CandidatePendingRetry {
			if err := stateDB.Upsert(sourceID, c.Path, c.Blake3, c.Mtime, c.Size); err != nil {
				return fmt.Errorf("updating state for %s: %w", c.Path, err)
			}
		}
	}

	totalEvents := len(candidates) + len(deleted)
	if totalEvents == 0 {
		fmt.Println("Nothing to sync.")
		printSummary(len(currentFiles), len(candidates), newCount, modCount, retryCount, len(deleted), 0, 0, start)
		return nil
	}

	// Step 5: Open gRPC stream.
	fmt.Println("Step 5: Connecting to server...")
	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connecting to server: %w", err)
	}
	defer conn.Close()

	md := metadata.Pairs("authorization", "Bearer "+token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	client := cafiv1.NewIndexerClient(conn)
	stream, err := client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("opening sync stream: %w", err)
	}

	// Send handshake before any file events.
	if err := stream.Send(&cafiv1.ClientMessage{
		Message: &cafiv1.ClientMessage_Handshake{
			Handshake: &cafiv1.Handshake{
				ClientVersion: "v0.1.0",
				SourceId:      sourceID,
			},
		},
	}); err != nil {
		return fmt.Errorf("sending handshake: %w", err)
	}

	// Step 6: Send events and handle ACKs concurrently.
	var acked atomic.Int64
	var sendErrs atomic.Int64
	recvDone := make(chan error, 1)
	var mu sync.Mutex

	// Receiver goroutine: processes ACKs and errors from the server.
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvDone <- err
				return
			}
			switch msg := resp.Message.(type) {
			case *cafiv1.ServerMessage_EventAck:
				acked.Add(1)
				path := msg.EventAck.GetPath()
				mu.Lock()
				if markErr := stateDB.MarkSent(sourceID, path); markErr != nil {
					log.Printf("Warning: failed to mark sent for %s: %v", path, markErr)
				}
				mu.Unlock()
			case *cafiv1.ServerMessage_SyncError:
				if msg.SyncError.GetFatal() {
					recvDone <- fmt.Errorf("fatal server error: %s", msg.SyncError.GetMessage())
					return
				}
				log.Printf("Server error (non-fatal): %s", msg.SyncError.GetMessage())
			}
		}
	}()

	// Send file events with bounded parallelism.
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for _, c := range candidates {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if sendErr := stream.Send(&cafiv1.ClientMessage{
				Message: &cafiv1.ClientMessage_FileEvent{
					FileEvent: &cafiv1.FileEvent{
						Blake3:    c.Blake3,
						Path:      c.Path,
						Mtime:     c.Mtime,
						Size:      c.Size,
						MimeType:  c.MimeType,
						EventType: cafiv1.EventType_EVENT_TYPE_UPSERT,
					},
				},
			}); sendErr != nil {
				sendErrs.Add(1)
				log.Printf("Error sending event for %s: %v", c.Path, sendErr)
			}
		}()
	}

	for _, p := range deleted {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if sendErr := stream.Send(&cafiv1.ClientMessage{
				Message: &cafiv1.ClientMessage_FileEvent{
					FileEvent: &cafiv1.FileEvent{
						Path:      p,
						EventType: cafiv1.EventType_EVENT_TYPE_DELETED,
					},
				},
			}); sendErr != nil {
				sendErrs.Add(1)
				log.Printf("Error sending delete for %s: %v", p, sendErr)
			}
		}()
	}

	wg.Wait()
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("closing send: %w", err)
	}

	// Wait for the receiver to finish (server closes stream after processing).
	select {
	case err := <-recvDone:
		if err != nil && err != io.EOF {
			log.Printf("Stream ended with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for ACKs")
	}

	// Remove state entries for deleted files after successful sync.
	for _, p := range deleted {
		mu.Lock()
		if delErr := stateDB.Delete(sourceID, p); delErr != nil {
			log.Printf("Warning: failed to delete state for %s: %v", p, delErr)
		}
		mu.Unlock()
	}

	// Step 7: Print summary.
	printSummary(len(currentFiles), len(candidates), newCount, modCount, retryCount,
		len(deleted), int(acked.Load()), int(sendErrs.Load()), start)
	return nil
}

func countByType(candidates []scanner.Candidate) (newCount, modCount, retryCount int) {
	for _, c := range candidates {
		switch c.Type {
		case scanner.CandidateNew:
			newCount++
		case scanner.CandidateModified:
			modCount++
		case scanner.CandidatePendingRetry:
			retryCount++
		}
	}
	return
}

func printSummary(total, candidates, newCount, modCount, retryCount, deleted, acked, sendErrs int, start time.Time) {
	fmt.Printf("\n=== Scan Summary ===\n")
	fmt.Printf("Scanned:     %d files\n", total)
	fmt.Printf("Candidates:  %d (%d new, %d modified, %d pending retry)\n",
		candidates, newCount, modCount, retryCount)
	fmt.Printf("Deleted:     %d\n", deleted)
	if acked > 0 || sendErrs > 0 {
		fmt.Printf("ACKed:       %d / %d\n", acked, candidates+deleted)
		fmt.Printf("Send errors: %d\n", sendErrs)
	}
	fmt.Printf("Duration:    %.1fs\n", time.Since(start).Seconds())
}
