\set ON_ERROR_STOP on

\! echo "Starting second part of the WeatherApp database configuration..."

-- 5. Configure application schema 
\i ddl/3_manage_schema.sql

-- 6. Create tables
\i ddl/4_create_tables.sql

-- 11. Crear usuarios finales (lo último, después de tener todo configurado)
\i users/5_create_users.sql

-- 12. Load API data
\i dml/6_load_api_data.sql

\! echo "Second part of the WeatherApp database configuration completed successfully."