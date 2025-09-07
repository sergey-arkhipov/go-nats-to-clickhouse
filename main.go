package main

import (
	"clhs-service/config"
	"clhs-service/logger"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
)

// Настройки батчинга
const (
	batchSize    = 1000
	batchTimeout = 5 * time.Second
)

func main() {
	// ========== Config setup ==========
	// 1. Сначала загружаем конфигурацию
	configFile := flag.String("config", "nats.yml", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configFile)
	if err != nil {
		// Если ошибка в загрузке конфига, используем стандартный логгер для вывода
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	// 2. Инициализируем логгер.
	// Передаем всю структуру LogConfig.
	// Логика выбора уровня и формата теперь внутри пакета logger.
	logger.Init(cfg.Log)

	// 3. Выводим конфигурационный баннер.
	logger.ConfigBanner(*cfg)

	// ========== Context setup ==========
	// 1. Context with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for OS interrupt signals (Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Shutdown signal received, starting graceful shutdown...")
		cancel()
	}()

	// ========== Connection setup ==========
	// 1. Подключение к NATS
	natsURL := cfg.Nats.URL
	nc, err := nats.Connect(natsURL)
	if err != nil {
		slog.Error("Ошибка подключения к NATS")
		os.Exit(1)
	} else {
		defer nc.Close()
		slog.Info("Успешно подключено к NATS по адресу:", "NATS_URL", natsURL)
	}
	// 2. Подключение к ClickHouse
	chConnect, err := connectToClickHouse(ctx, &cfg.ClickHouse)
	if err != nil {
		slog.Error("Ошибка подключения к ClickHouse", "Error:", err)
		os.Exit(1)
	} else {
		defer chConnect.Close()
		slog.Info("Успешно подключено к ClickHouse")
	}
	// ========== Main block ==========
	// Создание канала для передачи сообщений батчеру
	messagesCh := make(chan *nats.Msg, batchSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go batchProcessor(ctx, chConnect, messagesCh, &wg)

	// 4. Подписка на стрим и отправка сообщений в канал
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("JetStream недоступен, не могу продолжить")
	}

	const durableConsumerName = "nats-clickhouse-durable"
	const deliveryGroupName = "nats-clickhouse-delivery-group"
	natsSubject := cfg.Subjects[0]

	_, err = js.QueueSubscribe(natsSubject, deliveryGroupName, func(msg *nats.Msg) {
		messagesCh <- msg
	}, nats.Durable(durableConsumerName),
		nats.ManualAck(),
	)
	if err != nil {
		slog.Error("Ошибка подписки на JetStream", "Error", err, "Subject", natsSubject)
		os.Exit(1)

	}

	log.Printf("Сервис запущен и ожидает сообщения на топике '%s'...", natsSubject)

	// Ожидание завершения работы
	wg.Wait()
}

// ========== End of Main block ==========

// connectToClickHouse устанавливает соединение с базой данных ClickHouse
func connectToClickHouse(context context.Context, config *config.ClickHouseConfig) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:9000", config.Hostname)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: config.Username,
			Password: config.Password,
		},
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
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context); err != nil {
		return nil, err
	}
	return conn, nil
}

// batchProcessor принимает сообщения из канала, накапливает их и отправляет батчами
func batchProcessor(ctx context.Context, conn clickhouse.Conn, messagesCh <-chan *nats.Msg, wg *sync.WaitGroup) {
	defer wg.Done()

	buffer := make([]*nats.Msg, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-messagesCh:
			buffer = append(buffer, msg)
			if len(buffer) >= batchSize {
				sendBatch(ctx, conn, buffer)
				buffer = make([]*nats.Msg, 0, batchSize)
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				slog.Warn("Таймаут истек, отправляем оставшиеся сообщения")
				sendBatch(ctx, conn, buffer)
				buffer = make([]*nats.Msg, 0, batchSize)
			}
		case <-ctx.Done():
			// Handle graceful shutdown: flush any remaining messages
			if len(buffer) > 0 {
				slog.Info("Context canceled, flushing remaining messages before exit.")
				if err := sendBatch(ctx, conn, buffer); err != nil {
					slog.Error("Failed to flush final batch.", "error", err)
				}
			}
			slog.Info("Batch processor shutting down.")
			return
		}
	}
}

// sendBatch отправляет накопленные сообщения в ClickHouse
func sendBatch(context context.Context, conn clickhouse.Conn, messages []*nats.Msg) error {
	batch, err := conn.PrepareBatch(context, "INSERT INTO nats_data_stream_supprt (timestamp, subject, chat_id, sequence, metadata, data)")
	if err != nil {
		log.Printf("Ошибка подготовки пакета для вставки: %v", err)
		return err
	}

	for _, msg := range messages {
		// Извлекаем метаданные JetStream
		metadata, err := msg.Metadata()
		if err != nil {
			log.Printf("Ошибка получения метаданных JetStream: %v", err)
			continue
		}

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			log.Printf("Ошибка сериализации metadata: %v", err)
			metadataJSON = []byte("{}")
		}
		// Split the string by the dot character.
		parts := strings.Split(msg.Subject, ".")
		chatID := ""
		// Check if there are at least 4 parts to avoid an index out of bounds error.
		// The parts slice is 0-indexed, so the 4th segment is at index 3.
		if len(parts) >= 4 {
			chatID = parts[3]
			fmt.Printf("The chat ID is: %s\n", chatID)
		} else {
			fmt.Println("The string does not have a fourth segment.")
		}

		err = batch.Append(
			metadata.Timestamp,
			msg.Subject,
			chatID,
			metadata.Sequence.Stream,
			string(metadataJSON),
			string(msg.Data),
		)
		if err != nil {
			log.Printf("Ошибка добавления данных в пакет: %v", err)
			return err
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("Ошибка отправки пакета в ClickHouse: %v", err)
		return err
	}
	// Only if the batch is successfully sent, we acknowledge all messages.
	for _, msg := range messages {
		if err := msg.Ack(); err != nil {
			slog.Error("Failed to ack message", "error", err)
		}
	}

	log.Printf("Успешно отправлен батч из %d сообщений.", len(messages))
	return nil
}
