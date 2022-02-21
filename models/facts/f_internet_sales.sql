with customer as (select * from {{ ref('dim_customer')}} ),

product as (select * from {{ ref('dim_product')}}),

orderdetail as (select * from {{ ref('dim_time')}}),

source_data as (
select 
Sales_Order_Number,
Sales_Order_Line_Number,
sc.Customer_Key,
sp.Product_Key,
st.Date_Key as order_date_key,
  CASE
            WHEN sc.Yearly_Income < 40000 THEN 'Low'
            WHEN sc.Yearly_Income > 60000 THEN 'High'
            ELSE 'Moderate'
        END AS IncomeGroup,
  CASE
            WHEN Month(GetDate()) < Month(sc.Birth_Date)
                THEN DateDiff(yy,sc.Birth_Date,GetDate()) - 1
            WHEN Month(GetDate()) = Month(sc.Birth_Date)
            AND Day(GetDate()) < Day(sc.Birth_Date)
                THEN DateDiff(yy,sc.Birth_Date,GetDate()) - 1
            ELSE DateDiff(yy,sc.Birth_Date,GetDate())
        END AS Age,
Order_Quantity,
Unit_Price,
Product_Std_Cost,
Total_Cost,
Sales_Amount,
Tax_Amt
from {{ source('staging','stg_sales_detail')}} sis 
inner join orderdetail st on sis.order_date_id = st.date_key
inner join customer sc on sis.customer_Id = sc.customer_key
inner join product sp on sis.product_id = sp.product_key
)

select * from source_data