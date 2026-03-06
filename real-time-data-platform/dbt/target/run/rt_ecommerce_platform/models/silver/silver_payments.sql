
  
    

  create  table "Ecommerce"."silver"."silver_payments__dbt_tmp"
  
  
    as
  
  (
    

with ranked as (
  select *,
         row_number() over (partition by payment_id order by event_timestamp desc, ingestion_ts desc) as rn
  from "Ecommerce"."bronze"."stg_payments_bronze"
)

select
  payment_id,
  order_id,
  user_id,
  amount,
  currency,
  payment_method,
  payment_status,
  event_timestamp as payment_timestamp
from ranked
where rn = 1
  );
  