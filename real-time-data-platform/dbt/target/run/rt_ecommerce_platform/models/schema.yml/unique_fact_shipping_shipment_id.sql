select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    shipment_id as unique_field,
    count(*) as n_records

from "Ecommerce"."public_gold"."fact_shipping"
where shipment_id is not null
group by shipment_id
having count(*) > 1



      
    ) dbt_internal_test