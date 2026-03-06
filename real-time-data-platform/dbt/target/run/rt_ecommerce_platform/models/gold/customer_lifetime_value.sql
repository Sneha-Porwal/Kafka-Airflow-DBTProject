
  
    

  create  table "Ecommerce"."gold"."customer_lifetime_value__dbt_tmp"
  
  
    as
  
  (
    

select
  o.user_id,
  sum(p.amount) as lifetime_value,
  count(distinct o.order_id) as total_orders,
  min(o.order_date) as first_order_date,
  max(o.order_date) as last_order_date
from "Ecommerce"."gold"."fact_orders" o
left join "Ecommerce"."gold"."fact_payments" p
  on o.order_id = p.order_id
 and p.payment_status = 'SUCCESS'
group by 1
  );
  