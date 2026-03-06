

select
  event_id,
  event_timestamp,
  ingestion_ts,
  payload ->> 'product_id' as product_id,
  payload ->> 'sku' as sku,
  payload ->> 'product_name' as product_name,
  payload ->> 'category' as category,
  coalesce((payload ->> 'unit_price')::numeric, 0)::numeric(12,2) as unit_price,
  coalesce((payload ->> 'is_active')::boolean, true) as is_active
from "Ecommerce"."bronze"."events_raw"
where topic_name = 'product_catalog_events'