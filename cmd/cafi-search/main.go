package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/VincentChalnot/cafi/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

func main() {
	var dbURL string

	rootCmd := &cobra.Command{
		Use:   "cafi-search",
		Short: "Search the Cafi file index",
	}
	rootCmd.PersistentFlags().StringVar(&dbURL, "db", "", "PostgreSQL connection string (or set CAFI_DATABASE_URL)")

	var sourceFilter string
	var globFilter string

	pathsCmd := &cobra.Command{
		Use:   "paths",
		Short: "List indexed file paths",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dbURL == "" {
				dbURL = os.Getenv("CAFI_DATABASE_URL")
			}
			if dbURL == "" {
				return fmt.Errorf("database URL required: use --db flag or CAFI_DATABASE_URL env var")
			}

			ctx := context.Background()
			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer pool.Close()

			database := db.New(pool)

			likePattern := ""
			if globFilter != "" {
				likePattern = globToLike(globFilter)
			}

			rows, err := database.QueryFilePaths(ctx, sourceFilter, likePattern)
			if err != nil {
				return fmt.Errorf("querying file paths: %w", err)
			}

			// Group by source
			grouped := make(map[string][]db.FilePathRow)
			var sourceOrder []string
			for _, r := range rows {
				if _, exists := grouped[r.SourceID]; !exists {
					sourceOrder = append(sourceOrder, r.SourceID)
				}
				grouped[r.SourceID] = append(grouped[r.SourceID], r)
			}

			for _, src := range sourceOrder {
				files := grouped[src]
				var totalSize int64
				for _, f := range files {
					totalSize += f.Size
				}
				fileWord := "files"
				if len(files) == 1 {
					fileWord = "file"
				}
				fmt.Printf("Source: %s (%d %s, %s)\n", src, len(files), fileWord, humanSize(totalSize))
				for _, f := range files {
					mime := "unknown"
					if f.MimeType != nil {
						mime = *f.MimeType
					}
					fullPath := f.Folder + f.Filename
					fmt.Printf("  %-45s %-25s %s\n", fullPath, mime, humanSize(f.Size))
				}
				fmt.Println()
			}

			if len(rows) == 0 {
				fmt.Println("No files found.")
			}

			return nil
		},
	}
	pathsCmd.Flags().StringVar(&sourceFilter, "source", "", "Filter by source ID")
	pathsCmd.Flags().StringVar(&globFilter, "filter", "", "Glob pattern to filter paths")

	rootCmd.AddCommand(pathsCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// globToLike converts a glob pattern to a SQL LIKE pattern.
func globToLike(glob string) string {
	result := strings.ReplaceAll(glob, "*", "%")
	result = strings.ReplaceAll(result, "?", "_")
	return result
}

// humanSize formats a byte count as a human-readable string.
func humanSize(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case bytes >= tb:
		return fmt.Sprintf("%.1f TB", float64(bytes)/float64(tb))
	case bytes >= gb:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.0f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
