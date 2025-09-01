package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nats-io/nats.go"
)

// Определение структуры для входящего сообщения NATS
type NatsMessage struct {
	Metadata map[string]interface{} `json:"metadata"`
	Subject  string                 `json:"subject"`
	Data     map[string]interface{} `json:"data"`
}

func main() {
	// Настройки подключения. Для простоты, здесь они жестко заданы,
	// но в реальном приложении лучше использовать переменные окружения.
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	natsSubject := "data.stream"

	clickhouseHost := os.Getenv("CLICKHOUSE_HOST")
	if clickhouseHost == "" {
		clickhouseHost = "localhost"
	}

	// 1. Подключение к NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("Успешно подключено к NATS по адресу: %s", natsURL)

	// Подключение к ClickHouse
	chConnect, err := connectToClickHouse(clickhouseHost)
	if err != nil {
		log.Fatalf("Ошибка подключения к ClickHouse: %v", err)
	}
	defer chConnect.Close()
	log.Println("Успешно подключено к ClickHouse")

	// 3. Подписка на стрим и обработка сообщений
	// Используем JetStream, если доступно
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("JetStream недоступен, используем стандартную подписку: %v", err)

		// Стандартная подписка NATS
		_, err = nc.Subscribe(natsSubject, func(msg *nats.Msg) {
			processMessage(chConnect, msg.Data)
		})
		if err != nil {
			log.Fatalf("Ошибка подписки на NATS: %v", err)
		}
	} else {
		// Подписка на JetStream с автоматическим подтверждением
		_, err = js.Subscribe(natsSubject, func(msg *nats.Msg) {
			processMessage(chConnect, msg.Data)
			msg.Ack() // Подтверждение получения сообщения
		})
		if err != nil {
			log.Fatalf("Ошибка подписки на JetStream: %v", err)
		}
	}

	log.Printf("Сервис запущен и ожидает сообщения на топике '%s'...", natsSubject)

	// Запуск бесконечного цикла для поддержания работы сервиса
	select {}
}

// connectToClickHouse устанавливает соединение с базой данных ClickHouse
func connectToClickHouse(host string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:9000", host)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version uint64
			}{
				{Name: "nats-clickhouse-transfer", Version: 1},
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

// processMessage обрабатывает полученные данные и вставляет их в ClickHouse
func processMessage(conn clickhouse.Conn, data []byte) {
	var msg NatsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("Ошибка декодирования JSON: %v. Данные: %s", err, string(data))
		return
	}

	// Преобразование JSON-объектов в строки для вставки в ClickHouse
	metadataJSON, err := json.Marshal(msg.Metadata)
	if err != nil {
		log.Printf("Ошибка сериализации metadata: %v", err)
		metadataJSON = []byte("{}")
	}

	dataJSON, err := json.Marshal(msg.Data)
	if err != nil {
		log.Printf("Ошибка сериализации data: %v", err)
		dataJSON = []byte("{}")
	}

	// Вставка данных
	// Используем пакетную вставку (Batch) для лучшей производительности
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO nats_data (timestamp, subject, metadata, data)")
	if err != nil {
		log.Printf("Ошибка подготовки пакета для вставки: %v", err)
		return
	}

	// Используем время получения сообщения
	now := time.Now()

	err = batch.Append(
		now,
		msg.Subject,
		string(metadataJSON),
		string(dataJSON),
	)
	if err != nil {
		log.Printf("Ошибка добавления данных в пакет: %v", err)
		return
	}

	if err := batch.Send(); err != nil {
		log.Printf("Ошибка отправки пакета в ClickHouse: %v", err)
		return
	}

	log.Printf("Сообщение успешно перенесено. Subject: %s", msg.Subject)
}
