{{ config(materialized='table') }}

with valid_orders as (
  select
    o.order_id,
    o.user_id,
    o.product_id,
    o.order_timestamp::date as order_date,
    o.quantity,
    o.total_amount,
    o.order_status
  from {{ ref('silver_orders') }} o
  inner join {{ ref('dim_users') }} u
    on o.user_id = u.user_id
  inner join {{ ref('dim_products') }} p
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
