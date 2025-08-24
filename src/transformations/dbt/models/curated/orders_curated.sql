{{ config(materialized='table') }}
select order_id, customer_id, amount, created_at, 
       case when amount > 100 then 'High' else 'Low' end as value_segment
from {{ ref('orders_staging') }}