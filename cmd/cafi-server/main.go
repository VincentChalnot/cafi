package main

import (
	"context"
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
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
)

func main() {
	configPath := "/etc/cafi/config.yaml"
	if len(os.Args) > 2 && os.Args[1] == "--config" {
		configPath = os.Args[2]
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.Server.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	database := db.New(pool)

	if err := database.RunMigrations(ctx); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Println("Migrations completed")

	// Upsert users and sources
	for _, u := range cfg.Users {
		if err := database.UpsertUser(ctx, u.ID); err != nil {
			log.Fatalf("Failed to upsert user %s: %v", u.ID, err)
		}
		for _, s := range u.Sources {
			hash, err := bcrypt.GenerateFromPassword([]byte(s.Token), bcrypt.DefaultCost)
			if err != nil {
				log.Fatalf("Failed to hash token for source %s: %v", s.ID, err)
			}
			if err := database.UpsertSource(ctx, s.ID, u.ID, string(hash)); err != nil {
				log.Fatalf("Failed to upsert source %s: %v", s.ID, err)
			}
		}
	}
	log.Println("Users and sources synced")

	// Set up auth interceptor
	authInterceptor := auth.NewInterceptor(database)
	if err := authInterceptor.LoadTokens(ctx); err != nil {
		log.Fatalf("Failed to load tokens: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(authInterceptor.StreamInterceptor()),
	)
	cafiv1.RegisterIndexerServer(grpcServer, server.NewIndexerServer(database))

	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
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

	log.Printf("gRPC server listening on %s", cfg.Server.GRPCAddr)
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
