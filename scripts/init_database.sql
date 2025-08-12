/*
===================================================================
CREATE DATABASE and SCHEMA
===================================================================
Script Purpose:
This script creates a new database 'DataWarehouse' after checking if it already exists.
If the database 'DataWarehouse exists', it is dropped and recreated. Adsitionally, the
script sets uo three schemas within the database: 'bronze', 'silver' and 'gold'.

WARNING:
This script drops the entire 'Datawarehouse' database if it exixts. All the data
in the database will be permanently deleted. Proceed with caution and makesure you have a 
backup before running thus script.

*/

use master;
GO

--Drop and recreate the database 'Datawarehouse'
IF EXISTS (SELECT 1 FORM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- CREATE DATABASE DataWarehouse
CREATE DATABASE DataWarehouse;
GO 

USE DataWarehouse;
GO

--Create Schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
