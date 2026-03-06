select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select email
from "Ecommerce"."public_bronze"."stg_users_bronze"
where email is null



      
    ) dbt_internal_test