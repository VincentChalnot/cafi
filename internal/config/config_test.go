package config

import (
	"os"
	"testing"
)

func TestLoadConfigFromEnv(t *testing.T) {
	// Set environment variables for the test
	os.Setenv("PORT", "60000")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	defer os.Unsetenv("PORT")
	defer os.Unsetenv("DATABASE_URL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.GRPCAddr != ":60000" {
		t.Errorf("Expected GRPCAddr :60000, got %s", cfg.GRPCAddr)
	}
	if cfg.DatabaseURL != "postgres://test:test@localhost:5432/test" {
		t.Errorf("Expected DatabaseURL postgres://test:test@localhost:5432/test, got %s", cfg.DatabaseURL)
	}
}

func TestLoadConfigDefaultPort(t *testing.T) {
	// Only set DATABASE_URL, PORT should default to 50051
	os.Unsetenv("PORT")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	defer os.Unsetenv("DATABASE_URL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.GRPCAddr != ":50051" {
		t.Errorf("Expected GRPCAddr :50051, got %s", cfg.GRPCAddr)
	}
}

func TestLoadConfigMissingDatabaseURL(t *testing.T) {
	os.Unsetenv("DATABASE_URL")

	_, err := Load()
	if err == nil {
		t.Fatal("Expected error due to missing DATABASE_URL, got nil")
	}
}
