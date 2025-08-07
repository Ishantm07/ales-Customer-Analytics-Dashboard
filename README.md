# 🧾 Sales Data Analysis Project (Medallion Architecture)

This project implements a comprehensive **Sales Data Pipeline** utilizing **SQL** and adhering to the **Medallion Architecture** (Bronze, Silver, Gold layers). It also builds interactive **Power BI dashboards** and **Python visualizations** to provide business insights.

---

## 📊 Project Overview

### ✅ Data Source
- `cust_info.csv`: Customer data
- `prd_info.csv`: Product data
- `sales_details.csv`: Transaction data

### ✅ Architecture Used
- **Bronze Layer**: Raw ingestion tables
- **Silver Layer**: Cleaned and enriched views using JOINs
- **Gold Layer**: Aggregated data (e.g., monthly product revenue)

---

## 🔧 SQL Workflow

1. **Bronze Layer**  
   Raw tables created using:
   ```sql
   CREATE TABLE bronze.customers (...)
   CREATE TABLE bronze.products (...)
   CREATE TABLE bronze.sales (...)
# Sales-Customer-Analytics-Dashboard


2. **Silver Layer**
  Cleaned, joined view:  
  CREATE VIEW silver_sales_data AS
  SELECT ...
  FROM bronze.sales
  JOIN bronze.customers
  JOIN bronze.products;

3.  **Gold Layer**
  Monthly revenue per product
  CREATE VIEW gold_monthly_product_data AS
  SELECT ...
  FROM silver_sales_data
  GROUP BY product_line, month;


📈 Power BI Dashboard
Includes:

Total Revenue by Product Line

Monthly Sales Trend

Delayed vs On-Time Shipments

Top 10 Countries by Revenue

