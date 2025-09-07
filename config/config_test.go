package config_test

import (
	"clhs-service/config"
	"log"
	"net/url"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// removeTmpFile удаляет временный файл и логирует ошибку, если удаление не удалось.
func removeTmpFile(tmpFile *os.File) {
	if err := os.Remove(tmpFile.Name()); err != nil {
		log.Fatalf("failed to remove temporary file %s: %v", tmpFile.Name(), err)
	}
}

// createTempConfigFile создаёт временный YAML-файл с заданным содержимым.
func createTempConfigFile(t *testing.T, content string) *os.File {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)

	if content != "" {
		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
	}
	require.NoError(t, tmpFile.Close())

	return tmpFile
}

func TestLoadConfig(t *testing.T) {
	// Сбрасываем viper перед каждым тестом
	viper.Reset()

	t.Run("successful load", func(t *testing.T) {
		tmpFile := createTempConfigFile(t, `---
nats:
  url: nats://test:4222
clickhouse:
  url: ch://default:passw@host1
log:
  format: json
subjects:
  - "test.>"
`)
		defer removeTmpFile(tmpFile)

		cfg, err := config.Load(tmpFile.Name())
		require.NoError(t, err)
		ch, _ := url.Parse(cfg.ClickHouse.URL)
		assert.Equal(t, "nats://test:4222", cfg.Nats.URL)
		assert.Equal(t, "default", ch.User.Username())
		assert.Equal(t, "test.>", cfg.Subjects[0])
	})

	t.Run("successful load with environment variables", func(t *testing.T) {
		tmpFile := createTempConfigFile(t, `---
nats:
  url: nats://test:4222
clickhouse:
  url: ch://default:passw@host1
log:
  format: json
`)
		defer removeTmpFile(tmpFile)

		// Устанавливаем переменные окружения с проверкой ошибок
		if err := os.Setenv("NATS_URL", "nats://env:4222"); err != nil {
			t.Fatalf("failed to set NATS_URL: %v", err)
		}
		if err := os.Setenv("CLICKHOUSE_URL", "ch://user:password@host:9111"); err != nil {
			t.Fatalf("failed to set ENVIRONMENT: %v", err)
		}
		// Отменяем переменные окружения с проверкой ошибок
		defer func() {
			if err := os.Unsetenv("NATS_URL"); err != nil {
				t.Errorf("failed to unset NATS_URL: %v", err)
			}
			if err := os.Unsetenv("CLICKHOUSE_URL"); err != nil {
				t.Errorf("failed to unset CLICKHOUSE_URL: %v", err)
			}
		}()

		cfg, err := config.Load(tmpFile.Name())
		require.NoError(t, err)

		assert.Equal(t, "nats://env:4222", cfg.Nats.URL)
		assert.Equal(t, "ch://user:password@host:9111", cfg.ClickHouse.URL)
	})

	t.Run("parse failures", func(t *testing.T) {
		tests := []struct {
			name      string
			config    string
			expectErr string
		}{
			{
				"invalid yaml",
				"invalid: [",
				"failed to read config file",
			},
			{
				"invalid parsing",
				`---
nats:
  url nats://test:4222
`,
				"failed to parse config into struct: ",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tmpFile := createTempConfigFile(t, tt.config)
				defer removeTmpFile(tmpFile)

				_, err := config.Load(tmpFile.Name())
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
			})
		}
	})
}
