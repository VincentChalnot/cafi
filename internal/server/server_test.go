package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/VincentChalnot/cafi/internal/auth"
	"github.com/VincentChalnot/cafi/internal/db"
	cafiv1 "github.com/VincentChalnot/cafi/internal/proto/cafi/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestSyncFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start Postgres container
	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "test",
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_DB":       "test",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("Failed to start postgres: %v", err)
	}
	defer func() { _ = pgContainer.Terminate(ctx) }()

	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get mapped port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://test:test@%s:%s/test?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	database := db.New(pool)
	if err := database.RunMigrations(ctx); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Create test user and source with a known bcrypt token hash
	const testToken = "tok_test_1234567890"
	hash, err := bcrypt.GenerateFromPassword([]byte(testToken), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("Failed to hash token: %v", err)
	}

	if err := database.UpsertUser(ctx, "test-user"); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}
	if err := database.UpsertSource(ctx, "test-source", "test-user", string(hash)); err != nil {
		t.Fatalf("Failed to create source: %v", err)
	}

	// Set up auth interceptor and load tokens
	authInt := auth.NewInterceptor(database)
	if err := authInt.LoadTokens(ctx); err != nil {
		t.Fatalf("Failed to load tokens: %v", err)
	}

	// Start gRPC server on a random port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.StreamInterceptor(authInt.StreamInterceptor()))
	cafiv1.RegisterIndexerServer(grpcServer, NewIndexerServer(database))
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	// Connect gRPC client with bearer token
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	md := metadata.Pairs("authorization", "Bearer "+testToken)
	streamCtx := metadata.NewOutgoingContext(ctx, md)

	client := cafiv1.NewIndexerClient(conn)
	stream, err := client.Sync(streamCtx)
	if err != nil {
		t.Fatalf("Failed to open sync stream: %v", err)
	}

	// Send Handshake
	err = stream.Send(&cafiv1.ClientMessage{
		Message: &cafiv1.ClientMessage_Handshake{
			Handshake: &cafiv1.Handshake{
				ClientVersion: "test-v0.1",
				SourceId:      "test-source",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to send handshake: %v", err)
	}

	// Send UPSERT event
	err = stream.Send(&cafiv1.ClientMessage{
		Message: &cafiv1.ClientMessage_FileEvent{
			FileEvent: &cafiv1.FileEvent{
				Blake3:    "abc123def456",
				Path:      "/test/file.txt",
				Mtime:     1700000000,
				Size:      1024,
				MimeType:  "text/plain",
				EventType: cafiv1.EventType_EVENT_TYPE_UPSERT,
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to send upsert: %v", err)
	}

	// Receive ACK for upsert
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive upsert ack: %v", err)
	}
	ack := resp.GetEventAck()
	if ack == nil {
		t.Fatal("Expected EventAck, got something else")
	}
	if ack.Blake3 != "abc123def456" || ack.Path != "/test/file.txt" {
		t.Fatalf("Unexpected ack: %+v", ack)
	}

	// Send DELETED event
	err = stream.Send(&cafiv1.ClientMessage{
		Message: &cafiv1.ClientMessage_FileEvent{
			FileEvent: &cafiv1.FileEvent{
				Path:      "/test/file.txt",
				EventType: cafiv1.EventType_EVENT_TYPE_DELETED,
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to send delete: %v", err)
	}

	// Receive ACK for delete
	resp, err = stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive delete ack: %v", err)
	}
	ack = resp.GetEventAck()
	if ack == nil {
		t.Fatal("Expected EventAck for delete")
	}

	// Verify database state: file should be soft-deleted, so QueryFilePaths returns empty
	rows, err := database.QueryFilePaths(ctx, "test-source", "")
	if err != nil {
		t.Fatalf("Failed to query file paths: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("Expected 0 active file paths, got %d", len(rows))
	}

	// Close stream
	_ = stream.CloseSend()
}
