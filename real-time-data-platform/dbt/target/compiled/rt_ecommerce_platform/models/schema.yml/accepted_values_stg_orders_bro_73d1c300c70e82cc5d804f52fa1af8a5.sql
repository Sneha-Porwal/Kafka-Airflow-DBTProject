
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "Ecommerce"."public_bronze"."stg_orders_bronze"
    group by order_status

)

select *
from all_values
where value_field not in (
    'PLACED','CONFIRMED','CANCELLED','FULFILLED'
)


