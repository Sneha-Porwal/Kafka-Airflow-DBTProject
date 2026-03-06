
  
    

  create  table "Ecommerce"."gold"."fact_shipping__dbt_tmp"
  
  
    as
  
  (
    

with valid_shipping as (
  select
    s.shipment_id,
    s.order_id,
    s.shipping_status,
    s.carrier,
    s.shipped_timestamp::date as shipped_date,
    s.delivered_timestamp::date as delivered_date
  from "Ecommerce"."silver"."silver_shipping" s
  inner join "Ecommerce"."gold"."fact_orders" o
    on s.order_id = o.order_id
)

select
  shipment_id,
  order_id,
  shipping_status,
  carrier,
  shipped_date,
  delivered_date
from valid_shipping
  );
  