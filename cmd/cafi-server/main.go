package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/VincentChalnot/cafi/internal/auth"
	"github.com/VincentChalnot/cafi/internal/config"
	"github.com/VincentChalnot/cafi/internal/db"
	cafiv1 "github.com/VincentChalnot/cafi/internal/proto/cafi/v1"
	"github.com/VincentChalnot/cafi/internal/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
)

func main() {
	var rootCmd = &cobra.Command{Use: "cafi-server"}

	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(userCmd())
	rootCmd.AddCommand(sourceCmd())
	rootCmd.AddCommand(tokenCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func initDB(ctx context.Context) (*db.DB, *pgxpool.Pool, *config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	database := db.New(pool)
	if err := database.RunMigrations(ctx); err != nil {
		pool.Close()
		return nil, nil, nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return database, pool, cfg, nil
}

func serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the gRPC server",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			database, pool, cfg, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			// Set up auth interceptor
			authInterceptor := auth.NewInterceptor(database)
			if err := authInterceptor.LoadTokens(ctx); err != nil {
				log.Fatalf("Failed to load tokens: %v", err)
			}

			grpcServer := grpc.NewServer(
				grpc.StreamInterceptor(authInterceptor.StreamInterceptor()),
			)
			cafiv1.RegisterIndexerServer(grpcServer, server.NewIndexerServer(database))

			lis, err := net.Listen("tcp", cfg.GRPCAddr)
			if err != nil {
				log.Fatalf("Failed to listen: %v", err)
			}

			// Graceful shutdown
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
			go func() {
				sig := <-sigCh
				log.Printf("Received signal %v, shutting down gracefully...", sig)
				grpcServer.GracefulStop()
				cancel()
			}()

			log.Printf("gRPC server listening on %s", cfg.GRPCAddr)
			if err := grpcServer.Serve(lis); err != nil {
				fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
				os.Exit(1)
			}
		},
	}
}

func userCmd() *cobra.Command {
	userCmd := &cobra.Command{
		Use:   "user",
		Short: "Manage users",
	}

	userCmd.AddCommand(&cobra.Command{
		Use:   "add <username>",
		Short: "Create a new user",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			id, err := database.CreateUser(ctx, args[0])
			if err != nil {
				log.Fatalf("Failed to create user: %v", err)
			}
			fmt.Printf("User %q created (id=%d)\n", args[0], id)
		},
	})

	userCmd.AddCommand(&cobra.Command{
		Use:   "remove <username>",
		Short: "Remove a user",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			if err := database.DeleteUser(ctx, args[0]); err != nil {
				log.Fatalf("Failed to remove user: %v", err)
			}
			fmt.Printf("User %q removed\n", args[0])
		},
	})

	return userCmd
}

func sourceCmd() *cobra.Command {
	sourceCmd := &cobra.Command{
		Use:   "source",
		Short: "Manage sources",
	}

	var strategy string
	var localPath string

	createCmd := &cobra.Command{
		Use:   "create <userid> <code>",
		Short: "Create a new source",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			stratVal := parseStrategy(strategy)
			var pathPtr *string
			if localPath != "" {
				pathPtr = &localPath
			}

			if stratVal == 1 { // local
				if localPath == "" {
					return fmt.Errorf("--path is required when strategy is local")
				}
				if _, err := os.Stat(localPath); os.IsNotExist(err) {
					return fmt.Errorf("path %q does not exist on the server filesystem", localPath)
				}
			}

			id, err := database.CreateSource(ctx, userID, args[1], stratVal, pathPtr)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}
			fmt.Printf("Source %q created (id=%d)\n", args[1], id)
			return nil
		},
	}
	createCmd.Flags().StringVar(&strategy, "strategy", "none", "Source strategy (none|local|remote)")
	createCmd.Flags().StringVar(&localPath, "path", "", "Local path on the server filesystem (required for local strategy)")
	sourceCmd.AddCommand(createCmd)

	sourceCmd.AddCommand(&cobra.Command{
		Use:   "remove <userid> <code>",
		Short: "Remove a source",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			if err := database.DeleteSource(ctx, userID, args[1]); err != nil {
				return fmt.Errorf("failed to remove source: %w", err)
			}
			fmt.Printf("Source %q removed\n", args[1])
			return nil
		},
	})

	var updateStrategy string
	var updatePath string
	updateCmd := &cobra.Command{
		Use:   "update <userid> <code>",
		Short: "Update a source",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			stratVal := parseStrategy(updateStrategy)
			var pathPtr *string
			if updatePath != "" {
				pathPtr = &updatePath
			}

			if stratVal == 1 { // local
				if updatePath == "" {
					return fmt.Errorf("--path is required when strategy is local")
				}
				if _, err := os.Stat(updatePath); os.IsNotExist(err) {
					return fmt.Errorf("path %q does not exist on the server filesystem", updatePath)
				}
			}

			if err := database.UpdateSource(ctx, userID, args[1], stratVal, pathPtr); err != nil {
				return fmt.Errorf("failed to update source: %w", err)
			}
			fmt.Printf("Source %q updated\n", args[1])
			return nil
		},
	}
	updateCmd.Flags().StringVar(&updateStrategy, "strategy", "none", "Source strategy (none|local|remote)")
	updateCmd.Flags().StringVar(&updatePath, "path", "", "Local path on the server filesystem")
	sourceCmd.AddCommand(updateCmd)

	return sourceCmd
}

func tokenCmd() *cobra.Command {
	tokenCmd := &cobra.Command{
		Use:   "token",
		Short: "Manage tokens",
	}

	var expireAt string
	createCmd := &cobra.Command{
		Use:   "create <userid> <name>",
		Short: "Create a new token",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			if expireAt == "" {
				expireAt = "9999-12-31T23:59:59Z"
			}

			token, hash, err := generateToken()
			if err != nil {
				return fmt.Errorf("failed to generate token: %w", err)
			}

			id, err := database.CreateToken(ctx, userID, args[1], hash, expireAt)
			if err != nil {
				return fmt.Errorf("failed to create token: %w", err)
			}
			fmt.Printf("Token %q created (id=%d)\n", args[1], id)
			fmt.Printf("Token: %s\n", token)
			return nil
		},
	}
	createCmd.Flags().StringVar(&expireAt, "expire-at", "", "Expiration datetime (default: far future)")
	tokenCmd.AddCommand(createCmd)

	tokenCmd.AddCommand(&cobra.Command{
		Use:   "remove <userid> <name>",
		Short: "Remove a token",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			if err := database.DeleteToken(ctx, userID, args[1]); err != nil {
				return fmt.Errorf("failed to remove token: %w", err)
			}
			fmt.Printf("Token %q removed\n", args[1])
			return nil
		},
	})

	var refreshExpireAt string
	refreshCmd := &cobra.Command{
		Use:   "refresh <userid> <name>",
		Short: "Refresh a token (atomically replaces the hash)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			userID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}

			if refreshExpireAt == "" {
				refreshExpireAt = "9999-12-31T23:59:59Z"
			}

			token, hash, err := generateToken()
			if err != nil {
				return fmt.Errorf("failed to generate token: %w", err)
			}

			if err := database.RefreshToken(ctx, userID, args[1], hash, refreshExpireAt); err != nil {
				return fmt.Errorf("failed to refresh token: %w", err)
			}
			fmt.Printf("Token %q refreshed\n", args[1])
			fmt.Printf("New Token: %s\n", token)
			return nil
		},
	}
	refreshCmd.Flags().StringVar(&refreshExpireAt, "expire-at", "", "Expiration datetime (default: far future)")
	tokenCmd.AddCommand(refreshCmd)

	tokenCmd.AddCommand(&cobra.Command{
		Use:   "add-source <tokenid> <sourceid> [sourceid...]",
		Short: "Link source(s) to a token",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			tokenID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid token ID: %w", err)
			}

			var sourceIDs []int
			for _, a := range args[1:] {
				sid, err := strconv.Atoi(a)
				if err != nil {
					return fmt.Errorf("invalid source ID %q: %w", a, err)
				}
				sourceIDs = append(sourceIDs, sid)
			}

			if err := database.AddTokenSources(ctx, tokenID, sourceIDs); err != nil {
				return fmt.Errorf("failed to add sources to token: %w", err)
			}
			fmt.Printf("Added %d source(s) to token %d\n", len(sourceIDs), tokenID)
			return nil
		},
	})

	tokenCmd.AddCommand(&cobra.Command{
		Use:   "remove-source <tokenid> <sourceid> [sourceid...]",
		Short: "Unlink source(s) from a token",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()

			tokenID, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("invalid token ID: %w", err)
			}

			var sourceIDs []int
			for _, a := range args[1:] {
				sid, err := strconv.Atoi(a)
				if err != nil {
					return fmt.Errorf("invalid source ID %q: %w", a, err)
				}
				sourceIDs = append(sourceIDs, sid)
			}

			if err := database.RemoveTokenSources(ctx, tokenID, sourceIDs); err != nil {
				return fmt.Errorf("failed to remove sources from token: %w", err)
			}
			fmt.Printf("Removed %d source(s) from token %d\n", len(sourceIDs), tokenID)
			return nil
		},
	})

	return tokenCmd
}

func parseStrategy(s string) int {
	switch s {
	case "local":
		return 1
	case "remote":
		return 2
	default:
		return 0
	}
}

func generateToken() (string, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	token := base64.StdEncoding.EncodeToString(b)
	hash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return "", "", err
	}
	return token, string(hash), nil
}
