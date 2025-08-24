
  
    
    

    create  table
      "dev"."lakehouse"."orders_curated__dbt_tmp"
  
    as (
      
select order_id, customer_id, amount, created_at, 
       case when amount > 100 then 'High' else 'Low' end as value_segment
from "dev"."lakehouse"."orders_staging"
    );
  
  