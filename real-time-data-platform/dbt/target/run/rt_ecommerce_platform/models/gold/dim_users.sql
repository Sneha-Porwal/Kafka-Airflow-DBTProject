
  
    

  create  table "Ecommerce"."gold"."dim_users__dbt_tmp"
  
  
    as
  
  (
    

with users_from_events as (
  select
    user_id,
    email,
    full_name,
    city,
    country,
    signup_ts,
    is_active
  from "Ecommerce"."silver"."silver_users"
),
users_from_orders as (
  select distinct
    o.user_id,
    concat(o.user_id, '@unknown.local') as email,
    o.user_id as full_name,
    null::text as city,
    null::text as country,
    null::timestamptz as signup_ts,
    true as is_active
  from "Ecommerce"."silver"."silver_orders" o
),
merged as (
  select * from users_from_events
  union all
  select * from users_from_orders
),
ranked as (
  select
    *,
    row_number() over (
      partition by user_id
      order by
        case when email like '%@unknown.local' then 2 else 1 end,
        signup_ts desc nulls last
    ) as rn
  from merged
)

select
  user_id,
  email,
  full_name,
  city,
  country,
  signup_ts,
  is_active
from ranked
where rn = 1
  );
  