{{ config(
    materialized='table'
    )
}}


with geography as (
    select * from {{ ref('dim_geography')}}
),

source_data as (
select st_cust.customer_key,
st_geo.geography_key,
Case
when st_cust.gender = 'M' then 'Mr'
when (st_cust.gender = 'F' and st_cust.marital_status = 'M') then 'Mrs'
when (st_cust.gender = 'F' and st_cust.marital_status <> 'M') then 'Miss'
else st_cust.title
end tile,
st_cust.first_name,
case 
when st_cust.middle_name = 'NULL' then ''
else st_cust.middle_name
end middle_name,
st_cust.last_name,
st_cust.birth_Date,
st_cust.marital_status,
st_cust.gender,
st_cust.email_Address,
st_cust.yearly_income,
(st_cust.add_line_1||','||st_cust.add_line_2) as address,
st_cust.phone as contact_no
from {{ source('staging','stg_customer')}} st_cust left outer join geography st_geo
on st_cust.geography_key = st_geo.geography_key
)

select *
from source_data
