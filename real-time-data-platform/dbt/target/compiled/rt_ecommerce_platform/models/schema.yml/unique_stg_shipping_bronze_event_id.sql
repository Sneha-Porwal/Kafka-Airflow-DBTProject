
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "Ecommerce"."public_bronze"."stg_shipping_bronze"
where event_id is not null
group by event_id
having count(*) > 1


