

with products_from_events as (
  select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    is_active
  from "Ecommerce"."silver"."silver_products"
),
products_from_orders as (
  select distinct
    o.product_id,
    concat('SKU-', replace(o.product_id, 'PRD-', '')) as sku,
    o.product_id as product_name,
    'Unknown'::text as category,
    0::numeric(12,2) as unit_price,
    true as is_active
  from "Ecommerce"."silver"."silver_orders" o
),
merged as (
  select * from products_from_events
  union all
  select * from products_from_orders
),
ranked as (
  select
    *,
    row_number() over (
      partition by product_id
      order by
        case when category = 'Unknown' then 2 else 1 end,
        unit_price desc
    ) as rn
  from merged
)

select
  product_id,
  sku,
  product_name,
  category,
  unit_price,
  is_active
from ranked
where rn = 1