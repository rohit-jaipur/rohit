{{ config(materialized='table') }}

with source_data as (
select Sales_Territory_Key,
Sales_Territory_Region as sales_region,
Sales_Territory_Country as sales_country,
case
when lower(Sales_Territory_Country) = lower('United States') then 'US'
when lower(Sales_Territory_Country) = lower('Canada') then 'CA'
when lower(Sales_Territory_Country) = lower('France') then 'FR'
when lower(Sales_Territory_Country) = lower('Germany') then 'DE'
when lower(Sales_Territory_Country) = lower('United Kingdom') then 'GB'
when lower(Sales_Territory_Country) = lower('Australia') then 'AU'
else 'NA'
end sales_region_code,
Sales_Territory_Group
from {{ source('staging','stg_sales_territory')}}
)

select *
from source_data
