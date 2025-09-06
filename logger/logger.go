// logger/logger.go
package logger

import (
	"clhs-service/config"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
)

// Init initializes the global slog logger.
func Init(cfg config.LogConfig, useColors bool) {
	logLevel := parseLogLevel(cfg.Level)
	var output io.Writer = os.Stdout

	if cfg.Format == "json" {
		handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
			Level: logLevel,
		})
		slog.SetDefault(slog.New(handler))
	} else {
		handler := newCustomConsoleHandler(output, &slog.HandlerOptions{
			Level: logLevel,
		})
		slog.SetDefault(slog.New(handler))
	}
}

// ConfigBanner outputs the service configuration banner.
func ConfigBanner(cfg config.Config, useColors bool) {
	// ... (Ваш код для ConfigBanner остается без изменений) ...
	if cfg.Log.Format == "json" {
		slog.Info("Configuration Loaded", "config", cfg)
	} else {
		banner := fmt.Sprintf(
			"\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
			color.New(color.FgHiBlue).Sprint("===== Loaded config ========"),
			color.CyanString("%-12s: %s", "NATS_URL", cfg.Nats.URL),
			color.CyanString("%-12s: %s", "Clickhouse host", cfg.ClickHouse.Hostname),
			color.CyanString("%-12s: %s", "Clickhouse user", cfg.ClickHouse.Username),
			color.CyanString("%-12s: %s", "Clickhouse pass", cfg.ClickHouse.Password),
			color.CyanString("%-12s: %s", "Log format", cfg.Log.Format),
			color.CyanString("%-12s: %s", "Log level", cfg.Log.Level),
			color.New(color.FgHiBlue).Sprint("============================"),
		)
		fmt.Println("Starting service ..." + banner)
	}
}

// newCustomConsoleHandler creates a custom handler that formats logs for console with colors.
func newCustomConsoleHandler(w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	return &CustomConsoleHandler{
		handler: slog.NewTextHandler(w, &slog.HandlerOptions{
			Level:       opts.Level,
			ReplaceAttr: opts.ReplaceAttr,
		}),
	}
}

// CustomConsoleHandler is a wrapper around TextHandler to customize output.
type CustomConsoleHandler struct {
	handler slog.Handler
}

// Handle implements the slog.Handler interface.
func (h *CustomConsoleHandler) Handle(ctx context.Context, r slog.Record) error {
	// Customize the time and level output here.
	timeStr := r.Time.Format(time.DateTime)
	levelStr := colorizeLevel(r.Level, strings.ToUpper(r.Level.String()))

	// Print time and colored level directly.
	fmt.Fprintf(os.Stdout, "%s | %s | ", timeStr, levelStr)

	// Delegate the rest of the record to the default handler.
	// This ensures attributes (key=value pairs) are formatted correctly.
	r.Message = r.Message
	r.Level = slog.LevelInfo // Use a neutral level to avoid redundant level output
	r.Time = time.Time{}     // Clear time to prevent it from being printed again

	return h.handler.Handle(ctx, r)
}

// WithAttrs returns a new handler that is a copy of the current handler with the given attributes added.
func (h *CustomConsoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &CustomConsoleHandler{handler: h.handler.WithAttrs(attrs)}
}

// WithGroup returns a new handler that is a copy of the current handler with the given group name added.
func (h *CustomConsoleHandler) WithGroup(name string) slog.Handler {
	return &CustomConsoleHandler{handler: h.handler.WithGroup(name)}
}

// Enabled reports whether the handler handles a record with the given level and context.
func (h *CustomConsoleHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

// parseLogLevel converts a string to slog.Level.
func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}

// colorizeLevel maps a slog.Level to a colorized string using fatih/color.
func colorizeLevel(level slog.Level, text string) string {
	switch level {
	case slog.LevelError:
		return color.RedString(text)
	case slog.LevelWarn:
		return color.YellowString(text)
	case slog.LevelInfo:
		return color.GreenString(text)
	case slog.LevelDebug:
		return color.BlueString(text)
	default:
		return text
	}
}
