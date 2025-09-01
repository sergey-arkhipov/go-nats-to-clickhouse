# Сервис переноса данных из NATS в ClickHouse

Этот сервис на Go предназначен для чтения сообщений из NATS (NATS JetStream) и их сохранения в базу данных ClickHouse.к
Предварительные требования

Для запуска сервиса вам потребуется:

    Установленный Go (версия 1.21 или выше).

    Запущенный сервер NATS.

    Запущенный сервер ClickHouse.

## Структура данных

Сервис ожидает JSON-сообщения со следующей структурой:

```json
{
  "metadata": {
    "key1": "value1",
    "key2": "value2"
  },
  "subject": "название_топика",
  "data": {
    "fieldA": 123,
    "fieldB": "string"
  }
}
```

## Настройка базы данных ClickHouse

Перед запуском сервиса необходимо создать соответствующую таблицу в ClickHouse. Подключитесь к ClickHouse и выполните следующий SQL-запрос:

```sql
CREATE TABLE nats_data (
timestamp DateTime64(3),
subject String,
metadata String,
data String
) ENGINE = MergeTree()
ORDER BY (timestamp);
```

Это создаст таблицу, которая будет хранить время получения сообщения, его subject и содержимое metadata и data как строки JSON.

## Сборка и запуск

1. Клонируйте репозиторий:

```bash

git clone http://localhost:3000/your_username/your_project_name.git
cd your_project_name

```

2. Инициализируйте модуль Go и загрузите зависимости:

```bash
go mod init nats-clickhouse-transfer
go mod tidy

```

3. Запустите сервис:
   Вы можете использовать переменные окружения для настройки подключения:

```bash
NATS_URL="nats://localhost:4222" CLICKHOUSE_HOST="localhost" go run .

```

Если переменные не заданы, сервис будет использовать значения по умолчанию (nats://localhost:4222 и localhost).

## Отправка тестовых сообщений

Для тестирования вы можете использовать NATS CLI. Например:

```bash
nats pub data.stream '{"metadata":{"user":"test","id":123},"subject":"test.subject","data":{"value":42}}'
```

Сообщение должно быть успешно перенесено в вашу таблицу nats_data в ClickHouse.
