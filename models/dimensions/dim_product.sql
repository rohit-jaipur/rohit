{{ config(
    materialized='table'
    )
}}


with prod_category as (
    select * from {{ ref('dim_product_category')}}
),

prod_sub_category as (
    select * from {{ ref('dim_product_sub_category')}}
),

source_data as (
select product_key as product_key,
product_name as product_name,
case when lower(product_category_name) = lower('NULL') then 'NA'
else product_category_name
end prod_Cat_name,
case when lower(product_sub_category_name) = lower('NULL') then 'NA'
else product_sub_category_name
end prod_sub_cat_name,
sub_cat.prod_sub_cat_key as product_sub_category_key,
standard_cost,
color,
model_name,
product_description as prod_desc,
list_price,
size,
weight
from {{ source('staging','stg_product')}} stg_prod
left outer join prod_category prod_cat 
on lower('stg_prod.product_Category_name') = lower('prod_cat.product_cat_name') 
left outer join prod_sub_category sub_cat
on lower('stg_prod.product_sub_category_name') = lower('sub_cat.english_product_subcategory_name')
)

select *
from source_data
