{{ config(
    materialized='table'
    )
}}

with source_data as (
select stg_geo.Geography_Key,
stg_geo.City,
stg_geo.State_Province_Code,
stg_geo.State_Province_Name,
stg_geo.Country_Region_Code,
stg_geo.Country_Region_Name,
stg_geo.Postal_Code,
stg_geo.Sales_Territory_Region,
stg_geo.Sales_Territory_Country,
stg_geo.Sales_Territory_Group
from {{ source('staging','stg_geography')}} stg_geo
where (city is not null or city <> 'NULL')
)

select *
from source_data
