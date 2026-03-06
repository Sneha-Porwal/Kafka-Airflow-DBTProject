

select
  event_id,
  event_timestamp,
  ingestion_ts,
  payload ->> 'user_id' as user_id,
  payload ->> 'email' as email,
  payload ->> 'full_name' as full_name,
  payload ->> 'city' as city,
  payload ->> 'country' as country,
  nullif(payload ->> 'signup_ts', '')::timestamptz as signup_ts,
  coalesce((payload ->> 'is_active')::boolean, false) as is_active
from "Ecommerce"."bronze"."events_raw"
where topic_name = 'users_events'