/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_product
GO

CREATE VIEW gold.dim_product AS
SELECT 
	   ROW_NUMBER() OVER(ORDER BY pri.[prd_start_dt], pri.[prd_key]) AS product_key
      ,pri.[prd_id] AS product_id
	  ,pri.[prd_key] AS product_number
	  ,pri.[prd_nm] AS product_name
      ,pri.[cat_id] AS category_number
	  ,prc.cat AS category
	  ,prc.subcat AS subcategory
	  ,prc.maintenance AS maintenance
	  ,pri.[prd_cost] AS product_cost
      ,pri.[prd_line] AS product_line
      ,pri.[prd_start_dt] AS start_date
  FROM [SQLserverDWH].[silver].[cmt_prd_info] pri
  LEFT JOIN [silver].[erp_px_cat_g1v2] prc
  ON pri.cat_id = prc.id
  WHERE pri.prd_end_dt IS NULL
  GO
-- =============================================================================
-- Create Dimension: gold.dim_customer
-- =============================================================================
IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customer
GO

CREATE VIEW gold.dim_customer AS
SELECT 
	  ROW_NUMBER() OVER(ORDER BY ci.[cst_key]) AS customer_key
	  ,ci.[cst_id] AS customer_id
      ,ci.[cst_key] AS customer_number
      ,ci.[cst_firstname] AS first_name
      ,ci.[cst_lastname] AS last_name
	  ,lo.cntry AS country
      ,ci.[cst_marital_status] AS marital_status
	  ,CASE WHEN ci.[cst_gndr] <> 'n/a' THEN ci.[cst_gndr]
		ELSE COALESCE(cmi.gen, 'n/a')
		END AS gender
	  ,cmi.bdate AS birthdate
	  ,ci.[cst_create_date] AS create_date
  FROM [SQLserverDWH].[silver].[cmt_cust_info] ci
 LEFT JOIN [silver].[erp_cust_az12] cmi ON ci.cst_key = cmi.cid
 LEFT JOIN [silver].[erp_loc_a101] lo ON ci.cst_key = lo.cid
 GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT [sls_ord_num] AS order_number
	  ,dp.product_number 	
      ,dc.customer_key
      ,[sls_order_dt] AS order_date
      ,[sls_ship_dt] AS ship_date
      ,[sls_due_dt] AS due_date
      ,[sls_sales] AS sales_amount
      ,[sls_quantity] AS quantity
      ,[sls_price] AS price
  FROM [SQLserverDWH].[silver].[cmt_sales_details] sd
  LEFT JOIN [gold].[dim_product] dp
  ON sd.sls_prd_key = dp.product_number
  LEFT JOIN [gold].[dim_customer] dc
  ON sd.sls_cust_id = dc.customer_id;
  GO
