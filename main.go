package main

import (
	"clhs-service/config"
	"clhs-service/logger"
	"context"
	"encoding/json"
	"flag"
	"fmt" // Алиас для стандартной библиотеки log
	"log"
	"log/slog"
	"os"
	"sync"
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
	logger.Init(cfg.Log, os.Getenv("FORCE_COLORS") == "1")

	// 3. Выводим конфигурационный баннер.
	logger.ConfigBanner(*cfg, os.Getenv("FORCE_COLORS") == "1")

	natsURL := cfg.Nats.URL

	// Использование рабочего subject
	natsSubject := "test.>"

	// 1. Подключение к NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		slog.Error("Ошибка подключения к NATS")
	}
	defer nc.Close()
	slog.Info("Успешно подключено к NATS по адресу:", "NATS_URL", natsURL)

	// 2. Подключение к ClickHouse
	chConnect, err := connectToClickHouse(&cfg.ClickHouse)
	if err != nil {
		slog.Error("Ошибка подключения к ClickHouse")
	}
	defer chConnect.Close()
	slog.Info("Успешно подключено к ClickHouse")

	// 3. Создание канала для передачи сообщений батчеру
	messagesCh := make(chan *nats.Msg, batchSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go batchProcessor(chConnect, messagesCh, &wg)

	// 4. Подписка на стрим и отправка сообщений в канал
	js, err := nc.JetStream()
	if err != nil {
		slog.Error("JetStream недоступен, не могу продолжить")
	}

	const durableConsumerName = "nats-clickhouse-durable"
	const deliveryGroupName = "nats-clickhouse-delivery-group"

	_, err = js.QueueSubscribe(natsSubject, deliveryGroupName, func(msg *nats.Msg) {
		messagesCh <- msg
		err = msg.Ack() // Подтверждение получения сообщения
		if err != nil {
			slog.Error("Failed to ack message")
		}
	}, nats.Durable(durableConsumerName))
	if err != nil {
		slog.Error("Ошибка подписки на JetStream")
	}

	log.Printf("Сервис запущен и ожидает сообщения на топике '%s'...", natsSubject)

	// Ожидание завершения работы
	wg.Wait()
}

// connectToClickHouse устанавливает соединение с базой данных ClickHouse
func connectToClickHouse(config *config.ClickHouseConfig) (clickhouse.Conn, error) {
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
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return conn, nil
}

// batchProcessor принимает сообщения из канала, накапливает их и отправляет батчами
func batchProcessor(conn clickhouse.Conn, messagesCh <-chan *nats.Msg, wg *sync.WaitGroup) {
	defer wg.Done()

	buffer := make([]*nats.Msg, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-messagesCh:
			buffer = append(buffer, msg)
			if len(buffer) >= batchSize {
				sendBatch(conn, buffer)
				buffer = make([]*nats.Msg, 0, batchSize)
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				slog.Warn("Таймаут истек, отправляем оставшиеся сообщения")
				sendBatch(conn, buffer)
				buffer = make([]*nats.Msg, 0, batchSize)
			}
		case <-context.Background().Done():
			return
		}
	}
}

// sendBatch отправляет накопленные сообщения в ClickHouse
func sendBatch(conn clickhouse.Conn, messages []*nats.Msg) {
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO nats_data (timestamp, subject, metadata, data)")
	if err != nil {
		log.Printf("Ошибка подготовки пакета для вставки: %v", err)
		return
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

		err = batch.Append(
			time.Now(),
			msg.Subject,
			string(metadataJSON),
			msg.Data,
		)
		if err != nil {
			log.Printf("Ошибка добавления данных в пакет: %v", err)
			return
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("Ошибка отправки пакета в ClickHouse: %v", err)
		return
	}
	log.Printf("Успешно отправлен батч из %d сообщений.", len(messages))
}
