{{ config(materialized='view') }}

select
  event_id,
  event_timestamp,
  ingestion_ts,
  payload ->> 'payment_id' as payment_id,
  payload ->> 'order_id' as order_id,
  payload ->> 'user_id' as user_id,
  coalesce((payload ->> 'amount')::numeric, 0)::numeric(12,2) as amount,
  coalesce(payload ->> 'currency', 'USD') as currency,
  coalesce(payload ->> 'payment_method', 'CARD') as payment_method,
  coalesce(payload ->> 'payment_status', 'PENDING') as payment_status
from {{ source('bronze', 'events_raw') }}
where topic_name = 'payments_events'
