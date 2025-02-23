CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME;
	
	SET @start_time_batch = GETDATE();

	PRINT '==================================================';
	PRINT 'BEGIN PROCEDURE FOR LOADING THE SILVER LAYER TABLES';
	PRINT '==================================================';

	BEGIN TRY
		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [cmt_cust_info] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'LOADING CRP TABLES';
		PRINT '--------------------------------------------------';
		PRINT '>>Truncating table: cmt_cust_info';
		TRUNCATE TABLE [silver].[cmt_cust_info];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: cmt_cust_info';
		INSERT INTO [silver].[cmt_cust_info] (
			[cst_id],             -- Customer ID (Primary Key)
			[cst_key],            -- Unique customer key (Business Key)
			[cst_firstname],      -- Cleaned customer first name
			[cst_lastname],       -- Cleaned customer last name
			[cst_marital_status], -- Standardized marital status ('Single', 'Married', or 'n/a')
			[cst_gndr],           -- Standardized gender ('Male', 'Female', or 'n/a')
			[cst_create_date]     -- Latest customer creation date
		)
		SELECT 
			cst_id,									-- Keep the original customer ID
			cst_key,								-- Keep the original customer key
			TRIM(cst_firstname) AS cst_firstname,   -- Remove leading/trailing spaces from the first name
			TRIM(cst_lastname) AS cst_lastname,     -- Remove leading/trailing spaces from the last name
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  -- Convert 'S' to 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- Convert 'M' to 'Married'
				ELSE 'n/a'                                                -- If not recognized, default to 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'   -- Convert 'M' to 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- Convert 'F' to 'Female'
				ELSE 'n/a'                                     -- If not recognized, default to 'n/a'
			END AS cst_gndr,
			cst_create_date							 -- Keep the original customer creation date
		FROM (
			-- Deduplication logic using ROW_NUMBER() window function
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY [cst_id]                    -- Group by customer ID to handle duplicates
					ORDER BY [cst_create_date] DESC          -- Prioritize the most recent record
				) AS ROW_NUM
			FROM [bronze].[cmt_cust_info]                    -- Source table from the bronze layer
		) AS d_t
		WHERE 
			ROW_NUM = 1                                      -- Only select the latest record for each customer ID
			AND [cst_id] IS NOT NULL                         -- Exclude records with NULL customer IDs
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [cmt_prd_info] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE();
		PRINT '>>Truncating table: cmt_prd_info';
		TRUNCATE TABLE [silver].[cmt_prd_info];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: cmt_prd_info';

		-- Inserting cleaned and transformed product data from the bronze layer into the silver layer
		INSERT INTO silver.cmt_prd_info (
			prd_id,         -- Product ID (Primary Key)
			cat_id,         -- Extracted and standardized Category ID from the Product Key
			prd_key,        -- Cleaned Product Key (without category prefix)
			prd_nm,         -- Product Name
			prd_cost,       -- Product Cost (defaulting nulls to 0)
			prd_line,       -- Standardized Product Line (Category Description)
			prd_start_dt,   -- Product Start Date (Casted to DATE format)
			prd_end_dt      -- Product End Date (Derived using LEAD() function for SCD Type 2 logic)
		)
		SELECT [prd_id]
			  -- Extracting Category ID from the first 5 characters of prd_key and replacing hyphens with underscores
			  ,REPLACE(SUBSTRING([prd_key], 1, 5), '-', '_') AS [cat_id]
			  -- Extracting the actual Product Key from the 7th character onward
			  ,SUBSTRING([prd_key], 7, LEN([prd_key])) AS prd_key  
			  ,[prd_nm]
			  -- Replacing NULL values in Product Cost with 0
			  ,ISNULL([prd_cost], 0) AS [prd_cost]  
			  -- Standardizing Product Line categories
			  ,CASE		
				   WHEN UPPER(TRIM([prd_line])) = 'M' THEN 'Mountain' 
				   WHEN UPPER(TRIM([prd_line])) = 'S' THEN 'orher Sales' 
				   WHEN UPPER(TRIM([prd_line])) = 'R' THEN 'Road'
				   WHEN UPPER(TRIM([prd_line])) = 'T' THEN 'Touring'
				   ELSE 'n/a'
			   END AS [prd_line]
			   -- Converting the start date to DATE format for consistency
			  ,CAST([prd_start_dt] AS DATE) AS [prd_start_dt]  

			   -- This implements SCD Type 2 behavior by assigning an end date as the day before the next record's start date
			  ,CAST(LEAD([prd_start_dt]) OVER (
					PARTITION BY [prd_key]              -- Partition by product key to handle each product individually
					ORDER BY [prd_start_dt] ASC         -- Ordered chronologically by start date
			   ) - 1 AS DATE) AS [prd_end_dt]           -- Subtract 1 day from the next start date to set the end date for the current record

		  FROM [SQLserverDWH].bronze.cmt_prd_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [cmt_sales_details] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE();
		PRINT '>>Truncating table: cmt_sales_details';
		TRUNCATE TABLE [silver].[cmt_sales_details];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: cmt_sales_details';
		INSERT INTO silver.cmt_sales_details (
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
		SELECT [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,CASE
					WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) <> 8 THEN NULL
					ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE) 
				END AS [sls_order_dt]
			  ,CASE
					WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) <> 8 THEN NULL
					ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE) 
				END AS [sls_ship_dt]
			  ,CASE
					WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) <> 8 THEN NULL
					ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE) 
				END AS [sls_due_dt]
			  ,CASE
					WHEN [sls_sales] <= 0 OR [sls_sales] IS NULL OR [sls_sales] <> [sls_quantity] * ABS([sls_price])
						THEN [sls_quantity] * ABS([sls_price])
					ELSE [sls_sales]
				END AS [sls_sales]
			  ,[sls_quantity]
			  ,CASE 
					WHEN [sls_price] <= 0 OR [sls_price] IS NULL 
						THEN [sls_sales] / NULLIF([sls_quantity], 0)
					ELSE [sls_price]
				END AS [sls_price]
		  FROM [SQLserverDWH].bronze.[cmt_sales_details]
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'
		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [erp_cust_az12] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------------------------------------';
		PRINT '>>Truncating table: erp_cust_az12';
		TRUNCATE TABLE [silver].[erp_cust_az12];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: erp_cust_az12';
		INSERT INTO [silver].[erp_cust_az12] ([cid], [bdate], [gen])
		SELECT 
			   CASE		-- Remove 'NAS' prefix 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			   END AS cid
			  ,CASE		-- Set the dates that are bigger then today to NULL
				WHEN [bdate] > GETDATE()THEN NULL
				ELSE [bdate]
			   END AS [bdate]	
			  ,CASE		-- Normalize gender values
				WHEN UPPER(TRIM([gen])) IN ('F', 'Female') THEN 'Female'
				WHEN UPPER(TRIM([gen])) IN ('M', 'Male') THEN 'Male'
				ELSE 'n/a'
			   END AS [gen]
		  FROM [SQLserverDWH].[bronze].[erp_cust_az12]
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'
		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [silver].[erp_loc_a101] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE()
		PRINT '>>Truncating table: erp_loc_a101';
		TRUNCATE TABLE [silver].[erp_loc_a101];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: erp_loc_a101';
		INSERT INTO [silver].[erp_loc_a101] ([cid], [cntry])
		SELECT 
			   REPLACE([cid], '-', '') AS [cid]
			  ,CASE WHEN UPPER(TRIM([cntry])) IN ('US', 'UNITED STATES', 'USA') THEN 'United States'
					WHEN UPPER(TRIM([cntry])) IN ('DE', 'GERMANY') THEN 'Germany'
					WHEN [cntry] IS NULL OR [cntry] = '' THEN 'n/a'
					ELSE TRIM([cntry])
				END AS [cntry]
		  FROM [bronze].[erp_loc_a101]
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'
		-- ====================================================================================
		-- CLEAN AND LOAD DATA FROM BRONZE TO SILVER FOR [silver].[erp_px_cat_g1v2] TABLE
		-- ====================================================================================
		SET @start_time = GETDATE()
		PRINT '>>Truncating table: erp_px_cat_g1v2';
		TRUNCATE TABLE [silver].[erp_px_cat_g1v2];
		-- Insert cleaned and deduplicated customer data from the bronze layer into the silver layer
		PRINT '>>Loading data into table: erp_px_cat_g1v2';
		INSERT INTO [silver].[erp_px_cat_g1v2] ([id], [cat], [subcat], [maintenance])
		SELECT [id]
			  ,[cat]
			  ,[subcat]
			  ,[maintenance]
		  FROM [SQLserverDWH].[bronze].[erp_px_cat_g1v2]
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'


	END TRY

	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING THE SILVER LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS NVARCHAR);
       	PRINT 'ERROR PROCEDURE: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
	END CATCH
		
	SET @end_time_batch = GETDATE();
	PRINT '=============================================================';
	PRINT 'LOADING COMPLETE';
	PRINT '>> LOAD DURATION FOR ALL THE TABLES: ' + CAST(DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR(50)) + ' seconds';
	PRINT '=============================================================';

END


EXEC [silver].[load_silver]
