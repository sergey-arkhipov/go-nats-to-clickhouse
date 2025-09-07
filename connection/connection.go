// File: connection/connection.go

// Package connection for NATS and Clickhouse
package connection

import (
	"clhs-service/config"
	"context"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
)

// ConnectNATS establishes a connection to NATS.
func ConnectNATS(natsURL string) *nats.Conn {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "NATS_URL", natsURL, "error", err)
		os.Exit(1)
	}
	slog.Info("Successfully connected to NATS", "NATS_URL", natsURL)
	return nc
}

// ConnectClickHouse establishes a connection to ClickHouse.
func ConnectClickHouse(ctx context.Context, cfg *config.ClickHouseConfig) clickhouse.Conn {
	ch, err := url.Parse(cfg.URL)
	if err != nil {
		slog.Error("Cannot get connection info for Clickhouse", "config", cfg)
	}
	host := ch.Hostname()
	port := ch.Port()
	username := ch.User.Username()
	password, _ := ch.User.Password()
	opts := &clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: username,
			Password: password,
		},
		DialTimeout: time.Second * 30,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "nats-clickhouse-transfer", Version: "1"},
			},
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		slog.Error("Failed to connect to ClickHouse", "error", err)
		os.Exit(1)
	}

	if err := conn.Ping(ctx); err != nil {
		slog.Error("Failed to ping ClickHouse", "error", err)
		os.Exit(1)
	}

	slog.Info("Successfully connected to ClickHouse")
	return conn
}
