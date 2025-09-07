// File: connection/connection.go

// Package connection for NATS and Clickhouse
package connection

import (
	"clhs-service/config"
	"context"
	"fmt"
	"log/slog"
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

// connectToClickHouse устанавливает соединение с базой данных ClickHouse
// func connectToClickHouse(context context.Context, config *config.ClickHouseConfig) (clickhouse.Conn, error) {
// 	conn, err := clickhouse.Open(&clickhouse.Options{
// 		Addr: []string{fmt.Sprintf("%s:9000", config.Hostname)},
// 		Auth: clickhouse.Auth{
// 			Database: "default",
// 			Username: config.Username,
// 			Password: config.Password,
// 		},
// 		ClientInfo: clickhouse.ClientInfo{
// 			Products: []struct {
// 				Name    string
// 				Version string
// 			}{
// 				{Name: "nats-clickhouse-transfer", Version: "1"},
// 			},
// 		},
// 		Settings: clickhouse.Settings{
// 			"max_execution_time": 60,
// 		},
// 		Compression: &clickhouse.Compression{
// 			Method: clickhouse.CompressionLZ4,
// 		},
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	if err := conn.Ping(context); err != nil {
// 		return nil, err
// 	}
// 	return conn, nil
// }

// ConnectClickHouse establishes a connection to ClickHouse.
func ConnectClickHouse(ctx context.Context, cfg *config.ClickHouseConfig) clickhouse.Conn {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:9000", cfg.Hostname)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.Username,
			Password: cfg.Password,
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
