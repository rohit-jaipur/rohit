with
customer as 
(
    select * from {{ ref ('dim_customer')}}
)

select customer_key, email_address
from customer 
where (email_address like '%..%' 
       or email_address like '%@@%' 
       or email_address  like '%.@%' 
       or email_address like '%@.%')