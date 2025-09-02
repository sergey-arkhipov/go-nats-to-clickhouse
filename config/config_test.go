package config_test

import (
	"clhs-service/config"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// removeTmpFile удаляет временный файл и логирует ошибку, если удаление не удалось.
func removeTmpFile(tmpFile *os.File) {
	if err := os.Remove(tmpFile.Name()); err != nil {
		logrus.Errorf("failed to remove temporary file %s: %v", tmpFile.Name(), err)
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
  username: default
  password: passwd
  hostname: host1
log:
  format: json
`)
		defer removeTmpFile(tmpFile)

		cfg, err := config.Load(tmpFile.Name())
		require.NoError(t, err)

		assert.Equal(t, "nats://test:4222", cfg.Nats.URL)
		assert.Equal(t, "default", cfg.ClickHouse.Username)
	})

	t.Run("successful load with environment variables", func(t *testing.T) {
		tmpFile := createTempConfigFile(t, `---
nats:
  url: nats://test:4222
clickhouse:
  username: default
  password: passwd
  hostname: host1
log:
  format: json
`)
		defer removeTmpFile(tmpFile)

		// Устанавливаем переменные окружения с проверкой ошибок
		if err := os.Setenv("NATS_URL", "nats://env:4222"); err != nil {
			t.Fatalf("failed to set NATS_URL: %v", err)
		}
		if err := os.Setenv("CLICKHOUSE_HOSTNAME", "host_env"); err != nil {
			t.Fatalf("failed to set ENVIRONMENT: %v", err)
		}
		// Отменяем переменные окружения с проверкой ошибок
		defer func() {
			if err := os.Unsetenv("NATS_URL"); err != nil {
				t.Errorf("failed to unset NATS_URL: %v", err)
			}
			if err := os.Unsetenv("CLICKHOUSE_HOSTNAME"); err != nil {
				t.Errorf("failed to unset CLICKHOUSE_HOSTNAME: %v", err)
			}
		}()

		cfg, err := config.Load(tmpFile.Name())
		require.NoError(t, err)

		assert.Equal(t, "nats://env:4222", cfg.Nats.URL)
		assert.Equal(t, "host_env", cfg.ClickHouse.Hostname)
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
