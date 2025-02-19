-- ====================================================================================
-- TRUNCATE & FULL LOAD TABLES IN BRONZE SCHEMA (Staging Layer) / REFRESHING THE TABLES
-- ====================================================================================
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME;
	
	SET @start_time_batch = GETDATE();

	PRINT '==================================================';
	PRINT 'BEGIN PROCEDURE FOR LOADING THE BRONZE LAYER TBLES';
	PRINT '==================================================';

	BEGIN TRY
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'LOADING CRP TABLES';
		PRINT '--------------------------------------------------';
		PRINT '>> TRUNCATING TABLE: cmt_cust_info';
		TRUNCATE TABLE [bronze].[cmt_cust_info]
		PRINT '>> INSERTIND DATA INTO: cmt_cust_info';
		BULK INSERT [bronze].[cmt_cust_info]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

--======================================================================================

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: cmt_prd_info';
		TRUNCATE TABLE [bronze].[cmt_prd_info]
		PRINT '>> INSERTIND DATA INTO: cmt_prd_info';
		BULK INSERT [bronze].[cmt_prd_info]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

--======================================================================================

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: cmt_sales_details';
		TRUNCATE TABLE [bronze].[cmt_sales_details]
		PRINT '>> INSERTIND DATA INTO: cmt_sales_details';
		BULK INSERT [bronze].[cmt_sales_details]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

--======================================================================================

		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------------------------------------';
		PRINT '>> TRUNCATING TABLE: erp_cust_az12';
		TRUNCATE TABLE [bronze].[erp_cust_az12]
		PRINT '>> INSERTIND DATA INTO: erp_cust_az12';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

--======================================================================================

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101]
		PRINT '>> INSERTIND DATA INTO: erp_loc_a101';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

--======================================================================================

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
		PRINT '>> INSERTIND DATA INTO: erp_px_cat_g1v2';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\Users\ljupce.gligorov1979\Downloads\Youtube Courses\Data with Bana DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, --start from second row (first row is names of the columns)
			FIELDTERMINATOR = ',', -- fields separator
			TABLOCK -- locking the whole table whule executing
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '--------------------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING THE BRONZE LAYER'
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
GO
