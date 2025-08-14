/*
===================================================================
CREATE DATABASE and SCHEMA (PostgreSQL)
===================================================================
Script Purpose:
This script creates a new database 'DataWarehouse' after checking if it already exists.
If the database 'DataWarehouse' exists, it is dropped and recreated. Additionally, the
script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
This script drops the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. 
Proceed with caution and ensure you have a backup before running this script.
*/

-- 1. Terminate connections to the database (if exists) and drop it
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
      PERFORM pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = 'datawarehouse';
      EXECUTE 'DROP DATABASE datawarehouse';
   END IF;
END$$;

-- 2. Create the database
CREATE DATABASE datawarehouse;

-- 3. Connect to the new database (in psql or your tool, run: \c datawarehouse)

-- 4. Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

