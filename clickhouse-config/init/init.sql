-- ./clickhouse/init/init.sql
-- GRANT ALL ON *.* TO clhs WITH GRANT OPTION;
-- USE default;

-- Создание сырых таблиц для каждого стрима
-- Заметьте, схема соответствует полям, которые мы отправляем из Go-сервиса
-- Создание сырых таблиц для каждого стрима
CREATE TABLE IF NOT EXISTS nats_data_stream_supprt
(
    `timestamp` DateTime,
    `subject` String,
    `chat_id` String,
    `sequence` UInt64,
    `metadata` String,
    `data` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp,chat_id,sequence);

CREATE TABLE IF NOT EXISTS nats_data_stream_crmabc
(
    `timestamp` DateTime,
    `subject` String,
    `chat_id` String,
    `sequence` UInt64,
    `metadata` String,
    `data` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp,chat_id,sequence);

-- Создание общей таблицы проектов организации
CREATE TABLE IF NOT EXISTS nats_data_all_streams
(
    `timestamp` DateTime,
    `subject` String,
    `chat_id` String,
    `sequence` UInt64,
    `metadata` String,
    `data` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp,chat_id,sequence);

-- Перенос данных из таблиц логов в общую таблицу для организации
CREATE MATERIALIZED VIEW nats_data_stream_supprt_mv
TO nats_data_all_streams
AS SELECT
    timestamp,
    subject,
    chat_id,
    sequence,
    metadata,
    data
FROM nats_data_stream_supprt;

-- Перенос данных из таблиц логов в общую таблицу для организации

CREATE MATERIALIZED VIEW nats_data_stream_crmabc_mv
TO nats_data_all_streams
AS SELECT
    timestamp,
    subject,
    chat_id,
    sequence,
    metadata,
    data
FROM nats_data_stream_crmabc;

-- Создание финальной аналитической таблицы 
CREATE TABLE IF NOT EXISTS analitics_data
(
    `timestamp` DateTime,
    `subject` String,
    `chat_id` String,
    `metadata` JSON,
    `data` JSON,
    `message_text` String,
    `message_meta` String,
    `message_id` String,
    `message_timestamp` DateTime,
    `client_code` String,
    `project_code` String,
    `user_id` String,
    `session_id` String,
    `message_from` String,
    `message_to` String,
    `message_type` String,
    `message_context` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (client_code, project_code, user_id, session_id, timestamp);


-- Создание Materialized View, который записывает данные в финальную таблицу
CREATE MATERIALIZED VIEW IF NOT EXISTS analitics_data_mv
TO analitics_data
AS
SELECT
    `timestamp`,
    `subject` ,
    `chat_id` ,
    `metadata`,
    `data` ,
    JSONExtractString(data, 'text') AS message_text,
    JSONExtractString(data, 'meta') AS message_meta,
    JSONExtractString(data, 'id') AS message_id,
    JSONExtractUInt(data, 'timestamp') AS message_timestamp,
    splitByChar('.', subject)[1] AS client_code,
    splitByChar('.', subject)[2] AS project_code,
    splitByChar('.', subject)[3] AS user_id,
    splitByChar('.', subject)[4] AS session_id,
    splitByChar('.', subject)[5] AS message_from,
    splitByChar('.', subject)[6] AS message_to,
    splitByChar('.', subject)[7] AS message_type,
    splitByChar('.', subject)[8] AS message_context
FROM nats_data_all_streams;

