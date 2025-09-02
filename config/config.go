// Package config handles application configuration loading and validation.
// It supports YAML configuration files with required field validation
// and provides safe defaults where appropriate.
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// ClickHouseConfig defines the structure for the Clickhouse connect configuration
type ClickHouseConfig struct {
	Hostname string `mapstructure:"hostname"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// NatsConfig defines the structure for the NATS connect configuration
type NatsConfig struct {
	URL string `mapstructure:"url"`
}

// LogConfig defines the structure for the logging configuration.
type LogConfig struct {
	Format string `mapstructure:"format"`
	Level  string `mapstructure:"level"`
}

// Config defines the structure for the application configuration.
type Config struct {
	Nats       NatsConfig       `mapstructure:"nats"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Log        LogConfig        `mapstructure:"log"`
}

// Load loads the configuration using viper, supporting YAML and environment variables.
func Load(configPath string) (*Config, error) {
	// Initialize viper
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yml")

	// Enable environment variable overrides without prefix
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Unmarshal into Config struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config into struct: %w", err)
	}

	return &cfg, nil
}
