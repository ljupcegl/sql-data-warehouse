/*
==========================================================================
=					CREATE DATABASE AND SCHEMA							 =
==========================================================================
Script purpose
	This script creates new database named "SQLserverDWH" after checking if already exists
	If database exists, then is droped and recreated. Additionaly, the script sets tree schemas
	within the databas 'bronze', 'silver, 'gold'

WARNING
	RUNNING THIS SCRIPT WILL DROP "SQLserverDWH" DATABASE IF IT EXISTS.
	ALL THE DATA IN THE DATASET WILL BE PERMANENTLY DELETED.
	mAKE SHURE THAT YOU HAVE ALL THE DATA BACKUP BEFORE RUNNING THIS SCRIPT
*/


USE master;
GO

-- Drop Database if exists then recreate
IF EXISTS (SELECT 1 FROM sys.databases where name = 'SQLserverDWH') 
BEGIN 
	ALTER DATABASE SQLserverDWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE SQLserverDWH
END;
GO

-- Create Database 
CREATE DATABASE SQLserverDWH;
GO

USE SQLserverDWH;

-- Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
