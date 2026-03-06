

with ranked as (
  select *,
         row_number() over (partition by order_id order by event_timestamp desc, ingestion_ts desc) as rn
  from "Ecommerce"."bronze"."stg_orders_bronze"
)

select
  order_id,
  user_id,
  product_id,
  quantity,
  unit_price,
  total_amount,
  currency,
  order_status,
  event_timestamp as order_timestamp
from ranked
where rn = 1