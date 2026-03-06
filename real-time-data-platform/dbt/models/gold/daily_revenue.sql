{{ config(materialized='table') }}

select
  payment_date as date_day,
  sum(amount) as revenue,
  count(distinct order_id) as total_orders,
  avg(amount) as avg_order_value
from {{ ref('fact_payments') }}
where payment_status = 'SUCCESS'
group by 1
