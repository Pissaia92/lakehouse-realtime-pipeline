-- Configurar catalogo Iceberg
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3://iceberg-warehouse/',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin'
);

USE CATALOG iceberg_catalog;

-- Criar database
CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

-- Criar tabela Kafka para consumo
CREATE TABLE orders_kafka (
    order_id INT,
    customer_id INT,
    amount DOUBLE,
    created_at STRING,
    status STRING,
    region STRING,
    payment_method STRING,
    processing_time STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-iceberg',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

-- Criar tabela Iceberg
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount DOUBLE,
    created_at STRING,
    status STRING,
    region STRING,
    payment_method STRING,
    processing_time STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'iceberg',
    'format' = 'parquet'
);

-- Inserir dados do Kafka para o Iceberg
INSERT INTO orders 
SELECT 
    order_id,
    customer_id,
    amount,
    created_at,
    status,
    region,
    payment_method,
    processing_time,
    CURRENT_TIMESTAMP as event_time
FROM orders_kafka;