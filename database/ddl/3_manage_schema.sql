CREATE SCHEMA weatherapp_schema;
ALTER SCHEMA weatherapp_schema OWNER TO weatherapp_admin_1;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO weatherapp_admin_role;
GRANT USAGE ON SCHEMA weatherapp_schema TO weatherapp_read_write_role;
GRANT USAGE ON SCHEMA weatherapp_schema TO weatherapp_read_role;

ALTER DATABASE weatherapp_database SET search_path = weatherapp_schema;

ALTER DEFAULT PRIVILEGES IN SCHEMA weatherapp_schema
GRANT SELECT, INSERT ON TABLES TO weatherapp_read_write_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA weatherapp_schema
GRANT SELECT ON TABLES TO weatherapp_read_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA weatherapp_schema
GRANT USAGE, SELECT ON SEQUENCES TO weatherapp_read_write_role;

GRANT USAGE ON SCHEMA weatherapp_schema TO weatherapp_read_write_role, weatherapp_read_role;
GRANT CREATE ON SCHEMA weatherapp_schema TO weatherapp_admin_role;
