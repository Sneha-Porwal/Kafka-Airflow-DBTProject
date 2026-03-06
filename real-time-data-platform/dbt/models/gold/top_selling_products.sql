{{ config(materialized='table') }}

select
  d.product_id,
  d.product_name,
  sum(o.quantity) as units_sold,
  sum(o.total_amount) as gross_sales
from {{ ref('fact_orders') }} o
join {{ ref('dim_products') }} d
  on o.product_id = d.product_id
group by 1,2
order by units_sold desc
