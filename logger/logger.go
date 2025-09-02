// Package logger init colorful logger Stdout
package logger

import (
	"bytes"
	"clhs-service/config"
	"fmt"
	"os"
	"strings"

	"github.com/logrusorgru/aurora/v3"
	"github.com/sirupsen/logrus"
)

type ColorfulFormatter struct {
	TimestampFormat string // Теперь с большой буквы!
	ShowLevel       bool
	UpperLevel      bool
	ForceColors     bool
}

var forceColors bool // приватная переменная пакета

func (f *ColorfulFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	au := aurora.NewAurora(f.ForceColors || isTerminal())

	// Временная метка
	if f.TimestampFormat != "" {
		b.WriteString(au.Gray(12, entry.Time.Format(f.TimestampFormat)).String())
		b.WriteByte(' ')
	}

	// Уровень логирования
	if f.ShowLevel {
		level := entry.Level.String()
		if f.UpperLevel {
			level = strings.ToUpper(level)
		}
		b.WriteString(colorizeLevel(au, entry.Level, level))
		b.WriteByte(' ')
	}

	// Сообщение и поля
	b.WriteString(au.Bold(au.White(entry.Message)).String())
	if len(entry.Data) > 0 {
		b.WriteString(au.Gray(12, " || ").String())
		for k, v := range entry.Data {
			b.WriteString(colorizeField(au, k, v))
		}
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func colorizeLevel(au aurora.Aurora, level logrus.Level, text string) string {
	switch level {
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		return au.Red(text).String()
	case logrus.WarnLevel:
		return au.Yellow(text).String()
	case logrus.InfoLevel:
		return au.Green(text).String()
	default:
		return au.Blue(text).String()
	}
}

func colorizeField(au aurora.Aurora, key string, value any) string {
	keyPart := au.Cyan(key + "=").String()
	var valuePart string

	switch v := value.(type) {
	case string:
		valuePart = au.Green(v).String()
	case int, float64:
		valuePart = au.Yellow(fmt.Sprintf("%v", v)).String()
	case bool:
		valuePart = au.Magenta(fmt.Sprintf("%v", v)).String()
	default:
		valuePart = au.White(fmt.Sprintf("%v", v)).String()
	}

	return keyPart + valuePart + " "
}

func isTerminal() bool {
	fileInfo, _ := os.Stdout.Stat()
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

// Init инициализирует логгер
func Init(level logrus.Level, useColors bool) {
	forceColors = useColors
	logrus.SetLevel(level)
	logrus.SetFormatter(&ColorfulFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		ShowLevel:       true,
		UpperLevel:      true,
		ForceColors:     forceColors,
	})
}

// ConfigBanner выводит информацию о конфигурации
func ConfigBanner(config config.Config) {
	au := aurora.NewAurora(forceColors || isTerminal())
	banner := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
		au.BrightBlue("===== Loaded config ========"),
		au.Cyan(fmt.Sprintf("%-12s: %s", "NATS_URL", config.Nats.URL)),
		au.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse host", config.ClickHouse.Hostname)),
		au.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse user", config.ClickHouse.Username)),
		au.Cyan(fmt.Sprintf("%-12s: %s", "Clickhouse host", config.ClickHouse.Password)),
		au.Cyan(fmt.Sprintf("%-12s: %s", "Log format", config.Log.Format)),
		au.Cyan(fmt.Sprintf("%-12s: %s", "Log level", config.Log.Level)),
		au.BrightBlue("============================"),
	)
	logrus.Info("Starting service ...\n" + banner)
}
