{{ config(
    materialized='table', transient=false
         )
    
}}

with source_data as (
select Date_Key,
Full_Date_Alternate_Key as date_number,
Day_Number_Of_Week as calendar_week,
Day_Name_Of_Week as day_name,
Day_Number_Of_Month as calendar_Day,
--DayNumberOfYear,
Week_Number_Of_Year,
English_Month_Name as month_name,
case 
when lower(English_month_name) = lower('January') then 1
when lower(English_month_name) = lower('February') then 2
when lower(English_month_name) = lower('March') then 3
when lower(English_month_name) = lower('April') then 4
when lower(English_month_name) = lower('May') then 5
when lower(English_month_name) = lower('June') then 6
when lower(English_month_name) = lower('July') then 7
when lower(English_month_name) = lower('August') then 8
when lower(English_month_name) = lower('September') then 9
when lower(English_month_name) = lower('October') then 10
when lower(English_month_name) = lower('November') then 11
when lower(English_month_name) = lower('December') then 12
else 0
end calendar_month,
Calendar_Year
from {{ source('staging','stg_time')}}
)

select *
from source_data
