/*
==========================================================================
=                    CREATE DATABASE AND SCHEMA                           =
==========================================================================
Script purpose:
    - Creates a new database named "SQLserverDWH" after checking if it exists.
    - If the database exists, it is dropped and recreated.
    - Defines three schemas within the database: 'bronze', 'silver', 'gold'.

WARNING:
    - RUNNING THIS SCRIPT WILL DROP "SQLserverDWH" DATABASE IF IT EXISTS.
    - ALL DATA IN THE DATABASE WILL BE PERMANENTLY DELETED.
    - MAKE SURE YOU HAVE BACKED UP YOUR DATA BEFORE RUNNING THIS SCRIPT.
*/

USE master;
GO

-- Drop Database if it exists, then recreate
IF DB_ID('SQLserverDWH') IS NOT NULL
BEGIN 
    PRINT 'Dropping existing database SQLserverDWH...';
    ALTER DATABASE SQLserverDWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SQLserverDWH;
    PRINT 'Database SQLserverDWH dropped successfully.';
END;
GO

-- Create Database 
PRINT 'Creating new database SQLserverDWH...';
CREATE DATABASE SQLserverDWH;
GO

USE SQLserverDWH;
GO

-- Create schemas if they do not exist
PRINT 'Creating schemas: bronze, silver, gold...';

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Schema "bronze" created.';
END;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT 'Schema "silver" created.';
END;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT 'Schema "gold" created.';
END;

PRINT 'Database and schemas successfully created.';
GO
