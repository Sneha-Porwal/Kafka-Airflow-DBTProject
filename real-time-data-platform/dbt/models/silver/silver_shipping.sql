{{ config(materialized='table') }}

with ranked as (
  select *,
         row_number() over (partition by shipment_id order by event_timestamp desc, ingestion_ts desc) as rn
  from {{ ref('stg_shipping_bronze') }}
)

select
  shipment_id,
  order_id,
  carrier,
  shipping_status,
  estimated_delivery_date,
  shipped_timestamp,
  delivered_timestamp
from ranked
where rn = 1
