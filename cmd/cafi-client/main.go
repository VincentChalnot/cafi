package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ignore "github.com/sabhiram/go-gitignore"

	cafiv1 "github.com/VincentChalnot/cafi/internal/proto/cafi/v1"
	"github.com/VincentChalnot/cafi/internal/scanner"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	_ "github.com/mattn/go-sqlite3"
)

// xdgDataDir returns the XDG data directory for cafi.
func xdgDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "cafi")
}

// xdgConfigDir returns the XDG config directory for cafi.
func xdgConfigDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "cafi")
}

// clientDBPath returns the path to the client SQLite database.
func clientDBPath() string {
	if v := os.Getenv("CAFI_DB_URL"); v != "" {
		return v
	}
	return filepath.Join(xdgDataDir(), "cafi.db")
}

// ignoreFilePath returns the path to the blocklist file.
func ignoreFilePath() string {
	return filepath.Join(xdgConfigDir(), "ignore.txt")
}

// openClientDB opens the client SQLite database and creates tables if needed.
func openClientDB() (*sql.DB, error) {
	dbPath := clientDBPath()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening client db: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS config (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating config table: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS sources (
		source_code TEXT PRIMARY KEY,
		path TEXT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating sources table: %w", err)
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
	return db, nil
}

func getConfig(db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRow(`SELECT value FROM config WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func setConfig(db *sql.DB, key, value string) error {
	_, err := db.Exec(`INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?`,
		key, value, value)
	return err
}

func promptInput(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "cafi-client",
		Short: "Cafi content-addressable file indexer client",
	}

	rootCmd.AddCommand(connectCmd())
	rootCmd.AddCommand(sourceCmd())
	rootCmd.AddCommand(scanCmd())
	rootCmd.AddCommand(searchCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func connectCmd() *cobra.Command {
	var token string
	var noRemoteCheck bool

	cmd := &cobra.Command{
		Use:     "connect <server>",
		Aliases: []string{"add", "new"},
		Short:   "Configure the server connection",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			// Check existing config
			existing, _ := getConfig(db, "server")
			if existing != "" {
				fmt.Printf("Warning: existing connection to %q will be overwritten.\n", existing)
				answer := promptInput("Continue? [y/N] ")
				if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
					fmt.Println("Aborted.")
					return nil
				}
			}

			if token == "" {
				token = promptInput("Token: ")
				if token == "" {
					return fmt.Errorf("token is required")
				}
			}

			// Verify connectivity unless skipped
			skipVerify := noRemoteCheck || os.Getenv("CAFI_SKIP_VERIFY") == "1"
			if !skipVerify {
				fmt.Printf("Verifying connectivity to %s...\n", args[0])
				conn, err := grpc.NewClient(args[0],
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					return fmt.Errorf("failed to connect to server: %w", err)
				}
				defer conn.Close()

				client := cafiv1.NewIndexerClient(conn)
				// Attach token in Authorization metadata and call Ping
				md := metadata.Pairs("authorization", "Bearer "+token)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ctx = metadata.NewOutgoingContext(ctx, md)
				if _, err := client.Ping(ctx, &cafiv1.PingRequest{}); err != nil {
					return fmt.Errorf("server ping failed: %w", err)
				}
				fmt.Println("Connection and token verified.")
			}

			if err := setConfig(db, "server", args[0]); err != nil {
				return fmt.Errorf("saving server config: %w", err)
			}
			if err := setConfig(db, "token", token); err != nil {
				return fmt.Errorf("saving token config: %w", err)
			}
			fmt.Printf("Connected to %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&token, "token", "", "Authentication token")
	cmd.Flags().BoolVar(&noRemoteCheck, "no-remote-check", false, "Skip server connectivity check")
	return cmd
}

func sourceCmd() *cobra.Command {
	sourceCmd := &cobra.Command{
		Use:   "source",
		Short: "Manage local sources",
	}

	sourceCmd.AddCommand(sourceAddCmd())
	sourceCmd.AddCommand(sourceRemoveCmd())
	sourceCmd.AddCommand(sourceListCmd())
	sourceCmd.AddCommand(sourceRefreshTokenCmd())
	sourceCmd.AddCommand(sourceUpdatePathCmd())

	return sourceCmd
}

func sourceAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "add <source_code> <localpath>",
		Aliases: []string{"create", "new"},
		Short:   "Add a local source",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			// Check if source already exists
			var exists string
			err = db.QueryRow(`SELECT source_code FROM sources WHERE source_code = ?`, args[0]).Scan(&exists)
			if err == nil {
				return fmt.Errorf("source %q already exists", args[0])
			}

			absPath, err := filepath.Abs(args[1])
			if err != nil {
				return fmt.Errorf("resolving path: %w", err)
			}

			_, err = db.Exec(`INSERT INTO sources (source_code, path) VALUES (?, ?)`, args[0], absPath)
			if err != nil {
				return fmt.Errorf("adding source: %w", err)
			}
			fmt.Printf("Source %q added (path: %s)\n", args[0], absPath)
			return nil
		},
	}
}

func sourceRemoveCmd() *cobra.Command {
	var purgeRemote bool
	cmd := &cobra.Command{
		Use:     "remove <source_code>",
		Aliases: []string{"rm", "delete"},
		Short:   "Remove a local source",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			sourceCode := args[0]

			if purgeRemote {
				serverAddr, _ := getConfig(db, "server")
				token, _ := getConfig(db, "token")
				if serverAddr == "" || token == "" {
					return fmt.Errorf("server connection required for --purge-remote; run 'cafi-client connect --help'")
				}
				fmt.Printf("Purging remote data for source %q...\n", sourceCode)
				// Connect and send purge (via a sync stream with special handling)
				// For now, just log - full purge would need a dedicated RPC
				fmt.Printf("Warning: remote purge via gRPC not yet implemented (requires dedicated RPC)\n")
			}

			// Delete scan_state entries for this source
			_, err = db.Exec(`DELETE FROM scan_state WHERE source_id = ?`, sourceCode)
			if err != nil {
				return fmt.Errorf("deleting scan state: %w", err)
			}

			// Delete the source
			ct, err := db.Exec(`DELETE FROM sources WHERE source_code = ?`, sourceCode)
			if err != nil {
				return fmt.Errorf("removing source: %w", err)
			}
			rows, _ := ct.RowsAffected()
			if rows == 0 {
				return fmt.Errorf("source %q not found", sourceCode)
			}
			fmt.Printf("Source %q removed\n", sourceCode)
			return nil
		},
	}
	cmd.Flags().BoolVar(&purgeRemote, "purge-remote", false, "Also purge remote data for this source")
	return cmd
}

func sourceListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all local sources",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			rows, err := db.Query(`SELECT source_code, path FROM sources`)
			if err != nil {
				return fmt.Errorf("querying sources: %w", err)
			}
			defer rows.Close()

			fmt.Printf("%-20s %-40s\n", "CODE", "PATH")
			fmt.Printf("%-20s %-40s\n", "----", "----")
			for rows.Next() {
				var code, path string
				if err := rows.Scan(&code, &path); err != nil {
					return fmt.Errorf("scanning source: %w", err)
				}
				fmt.Printf("%-20s %-40s\n", code, path)
			}
			return nil
		},
	}
}

func sourceRefreshTokenCmd() *cobra.Command {
	var token string

	cmd := &cobra.Command{
		Use:   "refresh-token",
		Short: "Update the stored authentication token",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			if token == "" {
				token = promptInput("New token: ")
				if token == "" {
					return fmt.Errorf("token is required")
				}
			}

			if err := setConfig(db, "token", token); err != nil {
				return fmt.Errorf("saving token: %w", err)
			}
			fmt.Println("Token updated.")
			return nil
		},
	}
	cmd.Flags().StringVar(&token, "token", "", "New authentication token")
	return cmd
}

func sourceUpdatePathCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "update-path <source_code> <localpath>",
		Short: "Update the local path of a source",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openClientDB()
			if err != nil {
				return err
			}
			defer db.Close()

			absPath, err := filepath.Abs(args[1])
			if err != nil {
				return fmt.Errorf("resolving path: %w", err)
			}

			ct, err := db.Exec(`UPDATE sources SET path = ? WHERE source_code = ?`, absPath, args[0])
			if err != nil {
				return fmt.Errorf("updating source path: %w", err)
			}
			rows, _ := ct.RowsAffected()
			if rows == 0 {
				return fmt.Errorf("source %q not found", args[0])
			}
			fmt.Printf("Source %q path updated to %s\n", args[0], absPath)
			return nil
		},
	}
}

type sourceEntry struct {
	Code string
	Path string
}

func scanCmd() *cobra.Command {
	var parallelism int
	var dryRun bool
	var resetState bool
	var verbose bool

	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Scan all configured sources and sync to server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScan(parallelism, dryRun, resetState, verbose)
		},
	}
	cmd.Flags().IntVar(&parallelism, "parallelism", 4, "Number of concurrent event sends")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Scan only, do not connect to server or modify state")
	cmd.Flags().BoolVar(&resetState, "reset-state", false, "Clear scan_state before scanning")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Display every path scanned")
	return cmd
}

func searchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search indexed files (not yet implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Search is not yet implemented in this version.")
			return nil
		},
	}
	return cmd
}

func runScan(parallelism int, dryRun, resetState, verbose bool) error {
	clientDB, err := openClientDB()
	if err != nil {
		return err
	}
	defer clientDB.Close()

	// Read config
	serverAddr, _ := getConfig(clientDB, "server")
	token, _ := getConfig(clientDB, "token")

	if serverAddr == "" || token == "" {
		return fmt.Errorf("server connection not configured; run 'cafi-client connect --help'")
	}

	// Read sources
	rows, err := clientDB.Query(`SELECT source_code, path FROM sources`)
	if err != nil {
		return fmt.Errorf("reading sources: %w", err)
	}
	var sources []sourceEntry
	for rows.Next() {
		var s sourceEntry
		if err := rows.Scan(&s.Code, &s.Path); err != nil {
			rows.Close()
			return fmt.Errorf("scanning source row: %w", err)
		}
		sources = append(sources, s)
	}
	rows.Close()

	if len(sources) == 0 {
		return fmt.Errorf("no sources configured; run 'cafi-client source --help'")
	}

	// Reset state if requested
	if resetState {
		if _, err := clientDB.Exec(`DELETE FROM scan_state`); err != nil {
			return fmt.Errorf("clearing scan_state: %w", err)
		}
	}

	// Load blocklist
	var gitIgnore *ignore.GitIgnore
	ignPath := ignoreFilePath()
	if _, err := os.Stat(ignPath); err == nil {
		gitIgnore, err = ignore.CompileIgnoreFile(ignPath)
		if err != nil {
			return fmt.Errorf("loading blocklist from %s: %w", ignPath, err)
		}
	}

	// Open state DB (using scanner package)
	stateDBPath := clientDBPath()
	stateDB, err := scanner.OpenStateDB(stateDBPath)
	if err != nil {
		return fmt.Errorf("opening state db: %w", err)
	}
	defer stateDB.Close()

	start := time.Now()

	if !dryRun {
		// Test connection to server
		fmt.Printf("Testing connection to %s...\n", serverAddr)
		conn, err := grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("connecting to server: %w", err)
		}

		md := metadata.Pairs("authorization", "Bearer "+token)
		ctx := metadata.NewOutgoingContext(context.Background(), md)

		client := cafiv1.NewIndexerClient(conn)
		stream, err := client.Sync(ctx)
		if err != nil {
			conn.Close()
			return fmt.Errorf("opening sync stream: %w", err)
		}

		// Send handshake with first source
		handshake := &cafiv1.ClientMessage{
			Message: &cafiv1.ClientMessage_Handshake{
				Handshake: &cafiv1.Handshake{
					ClientVersion: "v1.0.0",
					SourceCode:    sources[0].Code,
				},
			},
		}
		if err := stream.Send(handshake); err != nil {
			conn.Close()
			return fmt.Errorf("sending handshake: %w", err)
		}
		if verbose {
			log.Printf("Sent: %v", handshake)
		}

		// Process all sources
		var totalFiles, totalCandidates, totalDeleted int
		var acked atomic.Int64
		var sendErrs atomic.Int64
		recvDone := make(chan error, 1)
		var mu sync.Mutex

		// Receiver goroutine
		go func() {
			for {
				resp, err := stream.Recv()
				if err != nil {
					recvDone <- err
					return
				}
				if verbose {
					log.Printf("Received: %v", resp)
				}
				switch msg := resp.Message.(type) {
				case *cafiv1.ServerMessage_EventAck:
					acked.Add(1)
					path := msg.EventAck.GetPath()
					// We need to determine which source this ACK belongs to
					mu.Lock()
					// Try each source - the path should only match one
					for _, src := range sources {
						_ = stateDB.MarkSent(src.Code, path)
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

		sem := make(chan struct{}, parallelism)
		var wg sync.WaitGroup

		for _, src := range sources {
			fmt.Printf("\nScanning source %q (%s)...\n", src.Code, src.Path)

			currentFiles, err := scanner.WalkDirectory(src.Code, src.Path, gitIgnore, verbose)
			if err != nil {
				log.Printf("Error walking %s: %v", src.Path, err)
				continue
			}

			totalFiles += len(currentFiles)
			fmt.Printf("  Found %d files\n", len(currentFiles))

			stateEntries, err := stateDB.GetAll(src.Code)
			if err != nil {
				return fmt.Errorf("reading state for %s: %w", src.Code, err)
			}

			candidates, deleted := scanner.DetectChanges(currentFiles, stateEntries)

			if err := scanner.ProcessCandidates(src.Path, candidates); err != nil {
				return fmt.Errorf("processing candidates for %s: %w", src.Code, err)
			}

			totalCandidates += len(candidates)
			totalDeleted += len(deleted)

			// Persist computed hashes
			for _, c := range candidates {
				if c.Type != scanner.CandidatePendingRetry {
					mu.Lock()
					if err := stateDB.Upsert(src.Code, c.Path, c.Blake3, c.Mtime, c.Size); err != nil {
						mu.Unlock()
						return fmt.Errorf("updating state for %s: %w", c.Path, err)
					}
					mu.Unlock()
				}
			}

			// Send events
			for _, c := range candidates {
				sem <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done() }()
					clientMsg := &cafiv1.ClientMessage{
						Message: &cafiv1.ClientMessage_FileEvent{
							FileEvent: &cafiv1.FileEvent{
								Blake3:     c.Blake3,
								Path:       c.Path,
								Mtime:      c.Mtime,
								Size:       c.Size,
								MimeType:   c.MimeType,
								EventType:  cafiv1.EventType_EVENT_TYPE_UPSERT,
								SourceCode: src.Code,
							},
						},
					}
					if sendErr := stream.Send(clientMsg); sendErr != nil {
						sendErrs.Add(1)
						log.Printf("Error sending event for %s: %v", c.Path, sendErr)
					} else if verbose {
						log.Printf("Sent: %v", clientMsg)
					}
				}()
			}

			for _, p := range deleted {
				sem <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done() }()
					clientMsg := &cafiv1.ClientMessage{
						Message: &cafiv1.ClientMessage_FileEvent{
							FileEvent: &cafiv1.FileEvent{
								Path:       p,
								EventType:  cafiv1.EventType_EVENT_TYPE_DELETED,
								SourceCode: src.Code,
							},
						},
					}
					if sendErr := stream.Send(clientMsg); sendErr != nil {
						sendErrs.Add(1)
						log.Printf("Error sending delete for %s: %v", p, sendErr)
					} else if verbose {
						log.Printf("Sent: %v", clientMsg)
					}
				}()
			}

			wg.Wait()

			// Remove state entries for deleted files
			for _, p := range deleted {
				mu.Lock()
				if delErr := stateDB.Delete(src.Code, p); delErr != nil {
					log.Printf("Warning: failed to delete state for %s: %v", p, delErr)
				}
				mu.Unlock()
			}
		}

		if err := stream.CloseSend(); err != nil {
			return fmt.Errorf("closing send: %w", err)
		}

		// Wait for receiver
		select {
		case err := <-recvDone:
			if err != nil && err != io.EOF {
				log.Printf("Stream ended with error: %v", err)
			}
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout waiting for ACKs")
		}

		conn.Close()

		fmt.Printf("\n=== Scan Summary ===\n")
		fmt.Printf("Total files:  %d\n", totalFiles)
		fmt.Printf("Candidates:   %d\n", totalCandidates)
		fmt.Printf("Deleted:      %d\n", totalDeleted)
		fmt.Printf("ACKed:        %d / %d\n", acked.Load(), totalCandidates+totalDeleted)
		fmt.Printf("Send errors:  %d\n", sendErrs.Load())
		fmt.Printf("Duration:     %.1fs\n", time.Since(start).Seconds())
	} else {
		// Dry run mode
		for _, src := range sources {
			fmt.Printf("\nScanning source %q (%s)...\n", src.Code, src.Path)

			currentFiles, err := scanner.WalkDirectory(src.Code, src.Path, gitIgnore, verbose)
			if err != nil {
				log.Printf("Error walking %s: %v", src.Path, err)
				continue
			}

			fmt.Printf("  Found %d files\n", len(currentFiles))

			stateEntries, err := stateDB.GetAll(src.Code)
			if err != nil {
				return fmt.Errorf("reading state for %s: %w", src.Code, err)
			}

			candidates, deleted := scanner.DetectChanges(currentFiles, stateEntries)
			if err := scanner.ProcessCandidates(src.Path, candidates); err != nil {
				return fmt.Errorf("processing candidates for %s: %w", src.Code, err)
			}

			fmt.Println("--- DRY RUN ---")
			for _, c := range candidates {
				fmt.Printf("  UPSERT %s:/%s (blake3=%s, mime=%s, size=%d)\n",
					src.Code, c.Path, c.Blake3, c.MimeType, c.Size)
			}
			for _, p := range deleted {
				fmt.Printf("  DELETED %s:/%s\n", src.Code, p)
			}
		}
		fmt.Printf("\nDuration: %.1fs\n", time.Since(start).Seconds())
	}
	return nil
}
