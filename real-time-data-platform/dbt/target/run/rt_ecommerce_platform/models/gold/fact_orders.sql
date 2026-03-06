
  
    

  create  table "Ecommerce"."gold"."fact_orders__dbt_tmp"
  
  
    as
  
  (
    

with valid_orders as (
  select
    o.order_id,
    o.user_id,
    o.product_id,
    o.order_timestamp::date as order_date,
    o.quantity,
    o.total_amount,
    o.order_status
  from "Ecommerce"."silver"."silver_orders" o
  inner join "Ecommerce"."gold"."dim_users" u
    on o.user_id = u.user_id
  inner join "Ecommerce"."gold"."dim_products" p
    on o.product_id = p.product_id
)

select
  order_id,
  user_id,
  product_id,
  order_date,
  quantity,
  total_amount,
  order_status
from valid_orders
  );
  