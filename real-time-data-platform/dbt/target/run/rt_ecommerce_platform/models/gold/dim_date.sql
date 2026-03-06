
  
    

  create  table "Ecommerce"."gold"."dim_date__dbt_tmp"
  
  
    as
  
  (
    

with date_series as (
  select generate_series(current_date - interval '365 day', current_date + interval '365 day', interval '1 day')::date as date_day
)

select
  to_char(date_day, 'YYYYMMDD')::int as date_sk,
  date_day,
  extract(isodow from date_day)::int as day_of_week,
  extract(month from date_day)::int as month,
  extract(quarter from date_day)::int as quarter,
  extract(year from date_day)::int as year,
  case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from date_series
  );
  