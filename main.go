package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	// Использование рабочего subject
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

	// 2. Создание канала для передачи сообщений батчеру
	messagesCh := make(chan *nats.Msg, batchSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go batchProcessor(chConnect, messagesCh, &wg)

	// 3. Подписка на стрим и отправка сообщений в канал
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream недоступен, не могу продолжить: %v", err)
	}

	const durableConsumerName = "nats-clickhouse-durable"
	const deliveryGroupName = "nats-clickhouse-delivery-group"

	_, err = js.QueueSubscribe(natsSubject, deliveryGroupName, func(msg *nats.Msg) {
		messagesCh <- msg
		msg.Ack() // Подтверждение получения сообщения
	}, nats.Durable(durableConsumerName))
	if err != nil {
		log.Fatalf("Ошибка подписки на JetStream: %v", err)
	}

	log.Printf("Сервис запущен и ожидает сообщения на топике '%s'...", natsSubject)

	// Ожидание завершения работы
	wg.Wait()
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
				log.Println("Таймаут истек, отправляем оставшиеся сообщения")
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
