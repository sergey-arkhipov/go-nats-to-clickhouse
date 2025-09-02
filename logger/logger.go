// Package logger setup loger
package logger

import (
	"clhs-service/config"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/logrusorgru/aurora/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var au aurora.Aurora

// colorizeLevel maps a zerolog.Level to a colorized string using aurora.
func colorizeLevel(level zerolog.Level, text string) string {
	// Your colorization logic
	switch level {
	case zerolog.ErrorLevel:
		return au.Red(text).String()
	case zerolog.WarnLevel:
		return au.Yellow(text).String()
	case zerolog.InfoLevel:
		return au.Green(text).String()
	case zerolog.DebugLevel, zerolog.TraceLevel:
		return au.Blue(text).String()
	default:
		return au.Gray(12, text).String()
	}
}

// Init initializes the zerolog logger with custom console output.
// It now takes a 'format' string to choose between console and JSON.
func Init(level zerolog.Level, useColors bool, format string) {
	zerolog.SetGlobalLevel(level)

	// Check if the format is JSON
	if format == "json" {
		log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
		return
	}

	// Default to console output
	au = aurora.NewAurora(useColors || isTerminal())
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		NoColor:    !useColors,
		TimeFormat: time.DateTime,
	}

	output.FormatLevel = func(i any) string {
		var text string
		if ll, ok := i.(string); ok {
			text = strings.ToUpper(ll)
		} else {
			text = "???"
		}
		return fmt.Sprintf("%-6s", colorizeLevel(zerolog.Level(level), text))
	}

	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

// isTerminal checks if the output is a terminal.
func isTerminal() bool {
	fileInfo, _ := os.Stdout.Stat()
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

func ConfigBanner(cfg config.Config, useColors bool) {
	auLocal := aurora.NewAurora(useColors || isTerminal())
	banner := fmt.Sprintf(
		"\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
		auLocal.BrightBlue("===== Loaded config ========"),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "NATS_URL", cfg.Nats.URL)),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse host", cfg.ClickHouse.Hostname)),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse user", cfg.ClickHouse.Username)),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse pass", cfg.ClickHouse.Password)),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "Log format", cfg.Log.Format)),
		auLocal.Cyan(fmt.Sprintf("%-12s: %s", "Log level", cfg.Log.Level)),
		auLocal.BrightBlue("============================"),
	)

	// A more idiomatic approach for JSON format would be to log the config as a JSON object
	if cfg.Log.Format == "json" {
		log.Info().
			Str("message", "Starting service...").
			Interface("config", cfg).
			Msg("Configuration Loaded")
	} else {
		log.Info().Msg("Starting service ..." + banner)
	}
}
