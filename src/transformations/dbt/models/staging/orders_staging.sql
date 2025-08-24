{{ config(materialized='table') }}
select order_id, customer_id, amount, created_at from {{ ref('orders_raw') }}
where amount > 0