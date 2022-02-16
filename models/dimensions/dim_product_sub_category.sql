{{ config(
    materialized='table'
    )
}}

with source_data as (
select product_subcategory_key as prod_sub_cat_key,
english_product_subcategory_name as prod_sub_Cat_name,
product_Category_key as prod_cat_key
from {{ source('staging','stg_product_subcategory')}}
)

select *
from source_data
