/*
============================================================
 Data Transformation Script (Bronze → Silver Layer)

 This script standardizes and cleans raw CRM/ERP data 
from the bronze schema and loads it into the silver schema.
 Steps include: trimming, deduplication, validation, 
 normalization of categorical values, and fixing invalid dates.
 ============================================================
*/

-- ============================================================
-- Customer Info (CRM)
-- Deduplicate customers, clean names, normalize marital status
-- and gender values, and keep only the latest record per customer.
-- ============================================================
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;



-- ============================================================
-- Product Info (CRM)
-- Standardize product keys, names, cost values, and product lines. 
-- Calculate product end dates based on the next start date.
-- ============================================================
TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ) - INTERVAL '1 day' AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;



-- ============================================================
-- Sales Details (CRM)
-- Clean invalid dates, fix sales/price inconsistencies,
-- and ensure calculated fields match logical constraints.
-- ============================================================
TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price 
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::text) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::text) != 8 
            THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;



-- ============================================================
-- Customer Info (ERP, AZ12)
-- Normalize customer IDs, fix invalid birthdates, 
-- and standardize gender values.
-- ============================================================
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS cid,
    CASE 
        WHEN bdate > CURRENT_DATE THEN NULL 
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male' 
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;



-- ============================================================
-- Location Info (ERP, A101)
-- Clean up customer IDs and normalize country names.
-- ============================================================
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = 'DE'           THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;



-- ============================================================
-- Product Categories (ERP, PX_CAT_G1V2)
-- Simple passthrough to silver with no transformation.
-- ============================================================
TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


