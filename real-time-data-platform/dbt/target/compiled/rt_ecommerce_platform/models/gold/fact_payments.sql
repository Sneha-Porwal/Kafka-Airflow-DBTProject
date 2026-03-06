

with valid_payments as (
  select
    p.payment_id,
    p.order_id,
    p.user_id,
    p.payment_timestamp::date as payment_date,
    p.amount,
    p.payment_method,
    p.payment_status
  from "Ecommerce"."silver"."silver_payments" p
  inner join "Ecommerce"."gold"."fact_orders" o
    on p.order_id = o.order_id
)

select
  payment_id,
  order_id,
  user_id,
  payment_date,
  amount,
  payment_method,
  payment_status
from valid_payments