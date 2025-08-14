/*
------------------------------------------------------------
Bronze Layer Data Ingestion Script:
------------------------------------------------------------
This script loads raw source data from CSV files into the 
bronze layer tables of the datawarehouse database.  
The process follows these steps for each table:
1. TRUNCATE – Clears all existing records in the target table.
2. COPY – Loads fresh data from the corresponding source CSV file.
Data is loaded from both CRM and ERP systems, ensuring that the 
bronze layer always contains the latest raw data.
------------------------------------------------------------
*/

-- Remove all existing records from bronze.crm_cust_info to prepare for fresh data load
TRUNCATE TABLE bronze.crm_cust_info;

-- Load customer information from CRM source CSV file into bronze.crm_cust_info table
COPY bronze.crm_cust_info
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_crm\cust_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Remove all existing records from bronze.crm_prd_info to prepare for fresh data load
TRUNCATE TABLE bronze.crm_prd_info;

-- Load product information from CRM source CSV file into bronze.crm_prd_info table
COPY bronze.crm_prd_info 
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_crm\prd_info.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Remove all existing records from bronze.crm_sales_details to prepare for fresh data load
TRUNCATE TABLE bronze.crm_sales_details;

-- Load sales details from CRM source CSV file into bronze.crm_sales_details table
COPY bronze.crm_sales_details 
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_crm\sales_details.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Remove all existing records from bronze.erp_cust_az12 to prepare for fresh data load
TRUNCATE TABLE bronze.erp_cust_az12;

-- Load customer information from ERP source CSV file into bronze.erp_cust_az12 table
COPY bronze.erp_cust_az12
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_erp\CUST_AZ12.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Remove all existing records from bronze.erp_loc_a101 to prepare for fresh data load
TRUNCATE TABLE bronze.erp_loc_a101;

-- Load location information from ERP source CSV file into bronze.erp_loc_a101 table
COPY bronze.erp_loc_a101
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_erp\LOC_A101.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Remove all existing records from bronze.erp_px_cat_g1v2 to prepare for fresh data load
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

-- Load product category information from ERP source CSV file into bronze.erp_px_cat_g1v2 table
COPY bronze.erp_px_cat_g1v2
FROM 'C:\Users\mauri\OneDrive\Documents\sql-datawarehouse-project\source_erp\PX_CAT_G1V2.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');








