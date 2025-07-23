\set ON_ERROR_STOP on

\! echo "Starting first part of the WeatherApp database configuration..."

-- 0. Clear previous database, schemas, and roles
\i 0_clear_all.sql

-- 1. Create roles
\i users/1_create_roles.sql

-- 2. Create first admin user
CREATE USER weatherapp_admin_1 WITH 
    PASSWORD '123'
    CREATEROLE
    IN ROLE weatherapp_admin_role;

-- 2.1 Grant privileges to the admin user
GRANT weatherapp_read_write_role TO weatherapp_admin_1 WITH ADMIN OPTION;
GRANT weatherapp_read_role TO weatherapp_admin_1 WITH ADMIN OPTION;

-- 3. Create the database
\i ddl/2_create_database.sql

\! echo "First part of the WeatherApp database configuration completed successfully."