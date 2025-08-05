SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'weatherapp_database'
  AND pid <> pg_backend_pid();

drop database if exists weatherapp_database;

DO $$
DECLARE
    role_record RECORD;
BEGIN
    FOR role_record IN SELECT rolname FROM pg_roles WHERE rolname LIKE 'weatherapp_%' AND rolname NOT IN ('postgres')
    LOOP
        EXECUTE 'DROP OWNED BY ' || quote_ident(role_record.rolname);
        EXECUTE 'DROP ROLE IF EXISTS ' || quote_ident(role_record.rolname);
    END LOOP;
END $$;