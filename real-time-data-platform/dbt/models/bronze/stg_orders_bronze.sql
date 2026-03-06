{{ config(materialized='view') }}

select
  event_id,
  event_timestamp,
  ingestion_ts,
  payload ->> 'order_id' as order_id,
  payload ->> 'user_id' as user_id,
  payload ->> 'product_id' as product_id,
  coalesce((payload ->> 'quantity')::int, 1) as quantity,
  coalesce((payload ->> 'unit_price')::numeric, 0)::numeric(12,2) as unit_price,
  coalesce((payload ->> 'total_amount')::numeric, 0)::numeric(12,2) as total_amount,
  coalesce(payload ->> 'currency', 'USD') as currency,
  coalesce(payload ->> 'order_status', 'PLACED') as order_status
from {{ source('bronze', 'events_raw') }}
where topic_name = 'orders_events'
