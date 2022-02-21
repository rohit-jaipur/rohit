{{ config(
    materialized='view'
    )
}}

with internet_sale as (select * from {{ ref('f_internet_sales')}}),

customer as (select * from {{ ref('dim_customer')}} ),

product as (select * from {{ ref('dim_product')}}),

orderdetail as (select * from {{ ref('dim_time')}}),

geography as (select * from {{ ref('dim_geography')}}),

product_category as (select * from {{ ref('dim_product_category')}}),

product_sub_category as (select * from {{ ref('dim_product_sub_category')}}),

sales_territory as (select * from {{ ref('dim_sales_territory')}}),

source_data as (
SELECT
        pc.prod_cat_name as product_category_name
        ,Coalesce(p.Model_Name, p.Product_Name) AS Model
        ,c.Customer_Key
        --,s.Sales_Territory_Group AS Region
        ,f.Age 
        ,f.IncomeGroup
        ,d.Calendar_Year
        ,d.month_name 
        ,f.Sales_Order_Number AS OrderNumber
        ,f.Sales_Order_Line_Number AS LineNumber
        ,f.Order_Quantity AS Quantity
        ,f.Total_Cost AS Amount 
from internet_sale f
    INNER JOIN orderdetail d
        ON f.Order_Date_Key = d.Date_Key
        INNER JOIN product p
        ON f.Product_Key = p.Product_Key
             INNER JOIN product_category pc
        ON p.prod_Cat_name = pc.prod_cat_name
            INNER JOIN customer c
        ON f.customer_Key = c.customer_Key
            INNER JOIN geography g
        ON c.Geography_Key = g.Geography_Key
            
)

select * from source_data
