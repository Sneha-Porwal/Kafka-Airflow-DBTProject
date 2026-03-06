{{ config(materialized='table') }}

with ranked as (
  select *,
         row_number() over (partition by product_id order by event_timestamp desc, ingestion_ts desc) as rn
  from {{ ref('stg_products_bronze') }}
)

select
  product_id,
  sku,
  product_name,
  category,
  unit_price,
  is_active,
  event_timestamp
from ranked
where rn = 1
