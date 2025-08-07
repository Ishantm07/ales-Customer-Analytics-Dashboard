create database datawarehouse;


create schema bronze;


create table bronze.customers(
customer_key int,
customer_id int,
customer_number nvarchar(50),
first_name nvarchar(50),
last_name nvarchar(50),
country nvarchar(50),
marital_status nvarchar(50),
gender nvarchar(50),
birth_date date,
create_date date);



create table bronze.products(
product_key int,
product_id int,
product_number nvarchar(50),
product_name nvarchar(50),
category_id int,
category nvarchar(50),
subcategory nvarchar(50),
maintenance nvarchar(50),
cost int,
product_line nvarchar(50),
start_date date
);


create table bronze.sales(
order_number nvarchar(50),
product_key int,
customer_key int,
order_date date,
shipping_date date,
due_date date,
sales_amount int,
quantity tinyint,
price int);


SHOW VARIABLES LIKE "secure_file_priv";
alter table bronze.customers
modify customer_key varchar(20);
alter table bronze.customers
modify customer_id varchar(20);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\cust_info.csv'
INTO TABLE bronze.customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, customer_key, first_name, last_name, 
country ,
marital_status ,
gender ,
birth_date ,
create_date) ;

select * from bronze.customers;
alter table bronze.customers 
drop column customer_number;
alter table bronze.products
modify product_key nvarchar(50);
alter table bronze.products
modify product_id nvarchar(50);
ALTER TABLE bronze.products
MODIFY COLUMN cost int ;


LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\prd_info.csv'
INTO TABLE bronze.products
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_key,
 product_id,
 product_number,
 product_name,
 @cost,
 product_line,
 start_date)
SET cost = NULLIF(@cost, '');

select * from bronze.products;





alter table bronze.products
drop column category_id ,
drop column category,

drop column subcategory;

alter table bronze.products
drop column maintenance;


ALTER TABLE bronze.sales
MODIFY COLUMN product_key nvarchar(50); 


LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sales_details.csv'
INTO TABLE bronze.sales
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
order_number ,
product_key ,
customer_key ,
order_date ,
shipping_date ,
due_date ,
sales_amount ,
quantity ,
price );

select * from bronze.sales; 





