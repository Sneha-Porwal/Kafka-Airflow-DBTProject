

with sessions as (
  select user_id, count(*) as order_events
  from "Ecommerce"."gold"."fact_orders"
  group by 1
),
paid as (
  select user_id, count(*) as paid_orders
  from "Ecommerce"."gold"."fact_payments"
  where payment_status = 'SUCCESS'
  group by 1
)

select
  s.user_id,
  s.order_events,
  coalesce(p.paid_orders, 0) as paid_orders,
  case when s.order_events = 0 then 0
       else round((coalesce(p.paid_orders, 0)::numeric / s.order_events::numeric) * 100, 2)
  end as conversion_rate_pct
from sessions s
left join paid p on s.user_id = p.user_id