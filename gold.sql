create schema gold;
use silver;

-- Sales Analysis
SHOW COLUMNS FROM silver_sales_data;

select * from silver_sales_data;

select
Month,
sum(sales_amount) as total_sales,
max(sales_over_time) as monthly_sales_over_check
from(
select date_format(order_date,'%Y-%m') as Month, sales_amount,sum(sales_amount)
over(partition by date_format(order_date,'%Y-%m') 
order by sales_amount desc) as sales_over_time
from silver_sales_data) as t
group by Month
order by Month desc;


select * from silver_sales_data;


select date_format(order_date,'%Y-%m') as month, sum(sales_amount) as total_sales
from silver_sales_data
group by `month`
order by `month` asc;

select country,
sum(sales_amount) as total_sales 
from silver_sales_data
group by country;

select product_line,
sum(sales_amount) as total_sales 
from silver_sales_data
group by product_line;

SELECT 
    trim(customer_name), SUM(sales_amount) AS total_sales
FROM
    silver_sales_data
GROUP BY trim(customer_name)
order by sum(sales_amount) desc;


select count(order_number) as total_order, 
avg(sales_amount) as avg_sales
from silver_sales_data;

with monthly_sales as(
select date_format(order_date,'%Y-%m') as monthly,
sum(sales_amount) as total_sales
from silver_sales_data
group by date_format(order_date,'%Y-%m'))
select monthly,
lag(total_sales) over(order by monthly) as last_month_sales,
total_sales-lag(total_sales) over(order by monthly) as m_o_m_changes
from monthly_sales;



with rnk_over as(
select *, row_number()
over(partition by order_number, 
order_date, shipping_date, 
sales_amount, quantity, price, customer_id, customer_name,
country, marital_status, gender, product_number,product_line, cost) as rnk
from silver_sales_data)
select count(customer_name) as repeated_orders
from rnk_over
where rnk >=2;


-- Product Performance

select * from silver_sales_data;

select product_line, count(order_number) as total_order,
sum(sales_amount) as total_sales
from silver_sales_data
group by product_line
order by count(order_number);


with total_sales as(
select sum(sales_amount) as total_sales
from silver_sales_data)

select product_line,
sum(sales_amount) as total_revenue,
ROUND(SUM(sales_amount) / t.total_sales * 100, 2) AS percent_revenue
from silver_sales_data
cross join
total_sales t
group by product_line, t.total_sales
order by percent_revenue desc;


select country,Product_line, avg(price) as avg_prc
from silver_sales_data
group by country,Product_line;

select country, (price-cost) as profit
from silver_sales_data
group by country,profit;

select Product_line, (price-cost) as profit
from silver_sales_data
group by Product_line,profit;


-- Customer Insights

select gender, count(customer_name) as total_count
from silver_sales_data
where gender is not null
group by gender;

select gender, marital_status, country,count(customer_name) as total_count
from silver_sales_data;

select trim(customer_name) as Customer_name,sum(sales_amount) as total_sales
from silver_sales_data
group by trim(customer_name);


with first_order as(
select trim(customer_name) as Customer_name,
min(order_date) as first_order
from silver_sales_data
group by trim(customer_name))
select 
trim(s.customer_name) as cust_name,
s.order_date,
case
  when s.order_date = f.first_order then 'New'
  when s.order_date > f.first_order then 'Returning'
  end as Customer_type
from silver_sales_data s
join first_order f on
trim(s.customer_name) = f.Customer_name; 

-- Order fullfillmet

select country,avg(datediff(shipping_date,order_date)) as avg_time 
from silver_sales_data
group by country;


select count(quantity) as total_orders,
case
	when datediff(shipping_date, order_date) > 7  then 'Delayed'
    when datediff(shipping_date, order_date) <= 7 then 'On-Time'
    else 'nothing'
    end as shipping_details
from silver_sales_data
group by case
	when datediff(shipping_date, order_date) > 7  then 'Delayed'
    when datediff(shipping_date, order_date) <= 7 then 'On-Time'
    else 'nothing'
    end;

select * from silver_sales_data;


create view gold_monthly_product_sales as
select date_format(order_date,'%Y-%m') as monthly,
product_line,
sum(sales_amount) as total_sales,
sum(quantity) as total_units_sold
from silver_sales_data
group by monthly, product_line;

select * from gold_monthly_product_sales;