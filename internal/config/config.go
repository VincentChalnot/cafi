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

// ServerConfig holds the server-level configuration.
type ServerConfig struct {
	GRPCAddr    string `mapstructure:"grpc_addr"`
	DatabaseURL string `mapstructure:"database_url"`
}

// Config is the top-level server configuration.
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Users  []UserConfig `mapstructure:"users"`
}

// Load reads and parses the config file at the given path.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}
	return &cfg, nil
}
