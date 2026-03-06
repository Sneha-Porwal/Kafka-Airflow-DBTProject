
  
    

  create  table "Ecommerce"."gold"."top_selling_products__dbt_tmp"
  
  
    as
  
  (
    

select
  d.product_id,
  d.product_name,
  sum(o.quantity) as units_sold,
  sum(o.total_amount) as gross_sales
from "Ecommerce"."gold"."fact_orders" o
join "Ecommerce"."gold"."dim_products" d
  on o.product_id = d.product_id
group by 1,2
order by units_sold desc
  );
  