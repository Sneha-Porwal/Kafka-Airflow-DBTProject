

select
  event_id,
  event_timestamp,
  ingestion_ts,
  payload ->> 'shipment_id' as shipment_id,
  payload ->> 'order_id' as order_id,
  payload ->> 'carrier' as carrier,
  payload ->> 'shipping_status' as shipping_status,
  nullif(payload ->> 'estimated_delivery_date', '')::date as estimated_delivery_date,
  nullif(payload ->> 'shipped_timestamp', '')::timestamptz as shipped_timestamp,
  nullif(payload ->> 'delivered_timestamp', '')::timestamptz as delivered_timestamp
from "Ecommerce"."bronze"."events_raw"
where topic_name = 'shipping_events'