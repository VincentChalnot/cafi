package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// SourceConfig describes a single source in the server config.
type SourceConfig struct {
	ID    string `mapstructure:"id"`
	Token string `mapstructure:"token"`
}

// UserConfig describes a single user and their sources.
type UserConfig struct {
	ID      string         `mapstructure:"id"`
	Sources []SourceConfig `mapstructure:"sources"`
}

// Config is the top-level server configuration.
type Config struct {
	GRPCAddr    string       `mapstructure:"grpc_addr"`
	DatabaseURL string       `mapstructure:"database_url"`
	Users       []UserConfig `mapstructure:"users"`
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetDefault("PORT", "50051")

	if err := v.BindEnv("PORT"); err != nil {
		return nil, fmt.Errorf("binding PORT: %w", err)
	}
	if err := v.BindEnv("DATABASE_URL"); err != nil {
		return nil, fmt.Errorf("binding DATABASE_URL: %w", err)
	}

	var cfg Config
	cfg.GRPCAddr = ":" + v.GetString("PORT")
	cfg.DatabaseURL = v.GetString("DATABASE_URL")

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	return &cfg, nil
}
