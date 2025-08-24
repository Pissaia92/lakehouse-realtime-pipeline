CREATE TABLE orders_raw (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  created_at TIMESTAMP(3),
  status STRING,
  region STRING,
  payment_method STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.orders_server.public.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE orders_curated (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  created_at TIMESTAMP(3),
  status STRING,
  region STRING,
  payment_method STRING,
  value_segment STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'hive_catalog',
  'uri' = 'thrift://minio:9083',
  'warehouse' = 's3a://iceberg-data/'
);

INSERT INTO orders_curated
SELECT order_id, customer_id, amount, created_at, status, region, payment_method,
       CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END AS value_segment
FROM orders_raw;