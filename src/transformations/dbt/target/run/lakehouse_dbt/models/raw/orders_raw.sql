
  
    
    

    create  table
      "dev"."lakehouse"."orders_raw__dbt_tmp"
  
    as (
      
SELECT *
FROM lakehouse_raw.sample_orders
    );
  
  