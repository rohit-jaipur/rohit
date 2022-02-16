{{ config(
    materialized='table'
    )
}}

with source_data as (
select product_Category_key as prod_cat_key,
product_cat_name as prod_cat_name
from {{ source('staging','stg_product_category')}}
)

select *
from source_data
