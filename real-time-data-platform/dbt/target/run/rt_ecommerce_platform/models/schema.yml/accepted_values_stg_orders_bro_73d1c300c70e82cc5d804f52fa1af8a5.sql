select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test