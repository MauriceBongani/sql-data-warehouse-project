-- TESTING THE crm_cust_info
--======================================================
SELECT 
cst_id,
COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id = NULL;

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


--------QUERY FOR DATA TRANSFORMATION AND DATA CLEANSING
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
    ELSE 'n/a'
END cst_marital_status,
CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
    ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info)
WHERE flag_last = 1;

----check for unwanted spaces 
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

--======================================================

--TESTING THE crm_prd_info table
--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT *
FROM bronze.crm_prd_info;

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key_suffix,
TRIM(prd_nm) AS prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -INTERVAL '1 day' AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


SELECT 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

--TESTING THE crm_sales_details table
--&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
SELECT 
sls_ord_num,
COUNT(*) AS ord_numbers
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING
COUNT(*)>1 AND sls_ord_num = NULL;

SELECT 
sls_ord_num,
    sls_prd_key,
    sls_cust_id,
CASE 
    WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
   ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
    sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;



SELECT 
NULLIF(sls_order_dt, '0')  sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8;

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

--TESTING THE QUALITY OF THE erp_cust table
--****************************************************************
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid
END AS cid,
bdate,
gen
 FROM bronze.erp_cust_az12
 WHERE 
 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid
END NOT IN 
(SELECT DISTINCT cst_key FROM silver.crm_cust_info);

 SELECT cid 
 FROM bronze.erp_cust_az12
 WHERE cid != TRIM(cid);

 SELECT * FROM silver.crm_cust_info;

 SELECT DISTINCT 
 CASE WHEN UPPER(TRIM(gen)) = 'F' THEN 'FEMALE'
    WHEN UPPER(TRIM(gen)) = 'M' THEN 'MALE'
   ELSE gen
END 
 FROM bronze.erp_cust_az12;

 SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
ELSE cid
END AS cid,
CASE WHEN bdate > CURRENT_DATE THEN NULL 
ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
    ELSE 'n/a'
END AS gen
 FROM bronze.erp_cust_az12;



 SELECT bdate
 FROM bronze.erp_cust_az12
 WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;


SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

--*************************************************************

--TESTING THE QUALITY OF THE erp_loc table
--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 
SELECT 
REPLACE(cid, '-', '') cid,
CASE 
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT 
CASE 
    WHEN UPPER(TRIM(cntry)) = 'US' THEN 'United States'
    WHEN UPPER(TRIM(cntry)) = 'USA' THEN 'United States'
    WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
    ELSE cntry
END cntry
FROM bronze.erp_loc_a101;

--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@''
