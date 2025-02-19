-- ================================================================
-- DROP & CREATE TABLES IN BRONZE SCHEMA (Staging Layer)
-- ================================================================

-- Customer Information Table
IF OBJECT_ID ('bronze.cmt_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.cmt_cust_info
	PRINT('DROPING THE TABLE bronze.cmt_cust_info')
CREATE TABLE bronze.cmt_cust_info (
    cst_id INT,  -- Customer ID (should be a primary key in final schema)
    cst_key NVARCHAR(50),  -- Unique customer identifier (potential business key)
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50), -- Gender
    cst_create_date DATE   -- Customer creation date
);
PRINT('CREATING THE TABLE bronze.cmt_cust_info')
GO
--=====================================================================
-- Product Information Table
IF OBJECT_ID ('bronze.cmt_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.cmt_prd_info
	PRINT('DROPING THE TABLE bronze.cmt_prd_info')
CREATE TABLE bronze.cmt_prd_info (
    prd_id INT,  -- Product ID
    prd_key NVARCHAR(50), -- Unique product key
    prd_nm NVARCHAR(50), -- Product name
    prd_cost INT,  -- Product cost (consider using DECIMAL for monetary values)
    prd_line NVARCHAR(50), -- Product line/category
    prd_start_dt DATETIME, -- Start date of the product availability
    prd_end_dt DATETIME  -- End date of the product availability
);
PRINT('CREATING THE TABLE bronze.cmt_prd_info')
GO
--========================================================================
-- Sales Details Table
IF OBJECT_ID ('bronze.cmt_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.cmt_sales_details
	PRINT('DROPING THE TABLE bronze.cmt_sales_details')
CREATE TABLE bronze.cmt_sales_details (
    sls_ord_num NVARCHAR(50), -- Order Number
    sls_prd_key NVARCHAR(50), -- Product Key (foreign key reference)
    sls_cust_id INT, -- Customer ID (foreign key reference)
    sls_order_dt INT, -- Order Date (changed from INT to DATE)
    sls_ship_dt INT,  -- Ship Date (changed from INT to DATE)
    sls_due_dt INT,  -- Due Date (changed from INT to DATE)
    sls_sales INT, -- Total Sales Amount (consider using DECIMAL for currency)
    sls_quantity INT, -- Quantity Sold
    sls_price INT  -- Price per Unit (consider using DECIMAL for monetary values)
);
PRINT('CREATING THE TABLE bronze.cmt_sales_details')
GO
--===========================================================================
-- ERP Customer Table (Raw Import)
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
	PRINT('DROPING THE TABLE bronze.erp_cust_az12')
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),  -- Customer ID (business key)
    bdate DATE,  -- Birth Date
    gen NVARCHAR(50)  -- Gender
);
PRINT('CREATING THE TABLE bronze.erp_cust_az12')
GO
--===========================================================================
-- ERP Location Data (Raw Import)
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
	PRINT('DROPING THE TABLE bronze.erp_loc_a101')
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),  -- Customer ID (foreign key reference)
    cntry NVARCHAR(50)  -- Country of residence
);
PRINT('CREATING THE TABLE bronze.erp_loc_a101')
GO
--===========================================================================
-- ERP Product Category Data (Raw Import)
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
	PRINT('DROPING THE TABLE bronze.erp_px_cat_g1v2')
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),  -- Product ID or Category Key
    cat NVARCHAR(50),  -- Product Category
    subcat NVARCHAR(50),  -- Product Subcategory
    maintenance NVARCHAR(50)  -- Maintenance Information
);
PRINT('CREATING THE TABLE bronze.erp_px_cat_g1v2')
GO
