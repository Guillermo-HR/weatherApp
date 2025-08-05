CREATE USER weatherapp_etl_1 WITH 
    PASSWORD '123'
    IN ROLE weatherapp_read_write_role;

CREATE USER weatherapp_reader_1 WITH 
    PASSWORD '123'
    IN ROLE weatherapp_read_role;