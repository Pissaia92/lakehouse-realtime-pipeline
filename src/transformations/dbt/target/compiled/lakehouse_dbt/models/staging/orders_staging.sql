
select order_id, customer_id, amount, created_at from "dev"."lakehouse"."orders_raw"
where amount > 0