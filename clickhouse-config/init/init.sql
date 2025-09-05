-- ./clickhouse/init/init.sql

-- Создание сырых таблиц для каждого стрима
-- Заметьте, схема соответствует полям, которые мы отправляем из Go-сервиса
-- Создание сырых таблиц для каждого стрима
CREATE TABLE IF NOT EXISTS nats_data_stream_project1
(
    `timestamp` DateTime,
    `subject` String,
    `sequence` UInt64,
    `metadata` JSON,
    `data` JSON
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY sequence;

CREATE TABLE IF NOT EXISTS nats_data_stream_project2
(
    `timestamp` DateTime,
    `subject` String,
    `sequence` UInt64,
    `metadata` JSON,
    `data` JSON
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY sequence;


-- Создание логической распределенной таблицы
CREATE TABLE IF NOT EXISTS nats_data_all_streams
(
    `timestamp` DateTime,
    `subject` String,
    `sequence` UInt64,
    `metadata` JSON,
    `data` JSON
)
ENGINE = Distributed(cluster_name, default, nats_data_stream_{name}, rand());


-- Создание финальной аналитической таблицы 
CREATE TABLE IF NOT EXISTS analitics_data
(
    `timestamp` DateTime,
    `subject` String,
    `data_value_key1` String,
    `data_value_key2` UInt64,
    `org_code` String,
    `project_code` String,
    `user_id` String,
    `session_id` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (org_code, project_code, user_id, session_id, timestamp);


-- Создание Materialized View, который записывает данные в финальную таблицу
CREATE MATERIALIZED VIEW IF NOT EXISTS analitics_data_mv
TO analitics_data
AS
SELECT
    timestamp,
    subject,
    JSONExtractString(data, 'key1') AS data_value_key1,
    JSONExtractUInt(data, 'key2') AS data_value_key2,
    splitByChar('.', subject)[1] AS org_code,
    splitByChar('.', subject)[2] AS project_code,
    splitByChar('.', subject)[3] AS user_id,
    splitByChar('.', subject)[4] AS session_id
FROM nats_data_all_streams;
