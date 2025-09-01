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

// Определение структуры для полезной нагрузки сообщения NATS
// (часть, которая содержится в msg.Data)
type NatsPayload struct {
	Data map[string]interface{} `json:"data"`
}

func main() {
	// Настройки подключения. Для простоты, здесь они жестко заданы,
	// но в реальном приложении лучше использовать переменные окружения.
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	natsSubject := "test.>"

	clickhouseHost := os.Getenv("CLICKHOUSE_HOST")
	if clickhouseHost == "" {
		clickhouseHost = "localhost"
	}
	clickhouseUser := os.Getenv("CLICKHOUSE_USERNAME")
	if clickhouseUser == "" {
		clickhouseUser = "default"
	}
	clickhousePassword := os.Getenv("CLICKHOUSE_PASSWORD")

	// 1. Подключение к NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Ошибка подключения к NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("Успешно подключено к NATS по адресу: %s", natsURL)

	// Подключение к ClickHouse
	chConnect, err := connectToClickHouse(clickhouseHost, clickhouseUser, clickhousePassword)
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
			processMessage(chConnect, msg)
		})
		if err != nil {
			log.Fatalf("Ошибка подписки на NATS: %v", err)
		}
	} else {
		// Подписка на JetStream с durable-консьюмером и группой доставки для надежности и масштабирования
		const durableConsumerName = "nats-clickhouse-durable"
		const deliveryGroupName = "nats-clickhouse-delivery-group"

		_, err = js.QueueSubscribe(natsSubject, deliveryGroupName, func(msg *nats.Msg) {
			processMessage(chConnect, msg)
			msg.Ack() // Подтверждение получения сообщения
		}, nats.Durable(durableConsumerName))
		if err != nil {
			log.Fatalf("Ошибка подписки на JetStream: %v", err)
		}

	}

	log.Printf("Сервис запущен и ожидает сообщения на топике '%s'...", natsSubject)

	// Запуск бесконечного цикла для поддержания работы сервиса
	select {}
}

// connectToClickHouse устанавливает соединение с базой данных ClickHouse
func connectToClickHouse(host, user, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:9000", host)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
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

// processMessage обрабатывает полученные данные и вставляет их в ClickHouse
func processMessage(conn clickhouse.Conn, msg *nats.Msg) {
	// Декодируем только полезную нагрузку
	var payload NatsPayload
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			log.Printf("Ошибка декодирования JSON: %v. Данные: %s", err, string(msg.Data))
			return
		}
	}

	// Вставка данных
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO nats_data (timestamp, subject, metadata, data)")
	if err != nil {
		log.Printf("Ошибка подготовки пакета для вставки: %v", err)
		return
	}

	// Задаем время вставки.
	now := time.Now()

	// Извлекаем метаданные из сообщения
	mtd, err := msg.Metadata()
	if err != nil {
		log.Printf("Ошибка получения метаданных: %v", err)
		return
	}

	err = batch.Append(
		now,
		msg.Subject,
		mtd,
		msg.Data,
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
