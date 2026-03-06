

with ranked as (
  select *,
         row_number() over (partition by user_id order by event_timestamp desc, ingestion_ts desc) as rn
  from "Ecommerce"."bronze"."stg_users_bronze"
)

select
  user_id,
  email,
  full_name,
  city,
  country,
  signup_ts,
  is_active,
  event_timestamp
from ranked
where rn = 1