use datawarehouse;

create schema gold;

select * from bronze.products;
select * from bronze.customers;
select * from bronze.sales;

SELECT 
  product_key,
  SUBSTRING_INDEX(product_key, '-', -3) AS extracted_part
FROM bronze.products;
SET SQL_SAFE_UPDATES = 0;

update bronze.products
set product_key = SUBSTRING_INDEX(product_key, '-', -3);


CREATE VIEW silver_summary_data AS
SELECT
    s.order_number,
    s.order_date,
    s.shipping_date,
    s.sales_amount,
    s.quantity,
    s.price,
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.country,
    c.marital_status,
    c.gender,
    p.product_number,
    p.product_line,
    p.cost
FROM bronze.sales s
JOIN bronze.customers c ON s.customer_key = c.customer_key
JOIN bronze.products p ON s.product_key = p.product_key;

select * from silver_summary_data;

select * from bronze.customers;
select * from bronze.products;

select * from silver_summary_data;