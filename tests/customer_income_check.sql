
with
customer as 
(
    select * from {{ ref ('dim_customer')}}
)

select
customer_key
from customer
where (yearly_income <= 0 or yearly_income is null)