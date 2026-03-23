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
		Use:   "create <userid>",
		Short: "Create a new user",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			if err := database.UpsertUser(ctx, args[0]); err != nil {
				log.Fatalf("Failed to create user: %v", err)
			}
			fmt.Printf("User %s created\n", args[0])
		},
	})

	userCmd.AddCommand(&cobra.Command{
		Use:   "remove <userid>",
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
			fmt.Printf("User %s removed\n", args[0])
		},
	})

	userCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List all users",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			users, err := database.ListUsers(ctx)
			if err != nil {
				log.Fatalf("Failed to list users: %v", err)
			}
			for _, user := range users {
				fmt.Println(user)
			}
		},
	})

	return userCmd
}

func sourceCmd() *cobra.Command {
	sourceCmd := &cobra.Command{
		Use:   "source",
		Short: "Manage sources",
	}

	sourceCmd.AddCommand(&cobra.Command{
		Use:   "create <userid> <sourceid>",
		Short: "Create a new source",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			token, hash, err := generateToken()
			if err != nil {
				log.Fatal(err)
			}

			if err := database.UpsertSource(ctx, args[1], args[0], hash); err != nil {
				log.Fatalf("Failed to create source: %v", err)
			}
			fmt.Printf("Source %s created for user %s\n", args[1], args[0])
			fmt.Printf("Token: %s\n", token)
		},
	})

	sourceCmd.AddCommand(&cobra.Command{
		Use:   "remove <sourceid>",
		Short: "Remove a source",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			if err := database.DeleteSource(ctx, args[0]); err != nil {
				log.Fatalf("Failed to remove source: %v", err)
			}
			fmt.Printf("Source %s removed\n", args[0])
		},
	})

	sourceCmd.AddCommand(&cobra.Command{
		Use:   "refresh-token <sourceid>",
		Short: "Refresh source token",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			token, hash, err := generateToken()
			if err != nil {
				log.Fatal(err)
			}

			if err := database.UpdateSourceToken(ctx, args[0], hash); err != nil {
				log.Fatalf("Failed to refresh token: %v", err)
			}
			fmt.Printf("Token refreshed for source %s\n", args[0])
			fmt.Printf("New Token: %s\n", token)
		},
	})

	sourceCmd.AddCommand(&cobra.Command{
		Use:   "ls",
		Short: "List all sources",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			database, pool, _, err := initDB(ctx)
			if err != nil {
				log.Fatal(err)
			}
			defer pool.Close()

			sources, err := database.ListSources(ctx)
			if err != nil {
				log.Fatalf("Failed to list sources: %v", err)
			}
			fmt.Printf("%-20s %-20s\n", "SOURCE ID", "USER ID")
			for _, s := range sources {
				fmt.Printf("%-20s %-20s\n", s.ID, s.UserID)
			}
		},
	})

	return sourceCmd
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
