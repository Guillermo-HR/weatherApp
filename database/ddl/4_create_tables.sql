create table weatherapp_schema.zone (
    id serial primary key,
    latitude float not null,
    longitude float not null,
    grid_size float not null,

    constraint zone_grid_size_check check (grid_size > 0),
    constraint zone_latitude_longitude_grid_size_uq unique (latitude, longitude, grid_size)
);

create table weatherapp_schema.api (
    id serial primary key,
    name text not null,

    constraint api_name_uq unique (name)
);

create table weatherapp_schema.weather (
    id serial primary key,
    temperature double precision not null,
    humidity double precision not null,
    pressure double precision not null,
    recorded_at timestamp with time zone default current_timestamp,
    zone_id integer not null,
    api_id integer not null,

    constraint weather_zone_id_fkey foreign key (zone_id) references weatherapp_schema.zone(id),
    constraint weather_api_id_fkey foreign key (api_id) references weatherapp_schema.api(id)
);
CREATE INDEX idx_weather_recorded_at ON weatherapp_schema.weather(recorded_at);

create table weatherapp_schema.air_quality (
    id serial primary key,
    co float not null,
    no float not null,
    no2 float not null,
    o3 float not null,
    so2 float not null,
    pm2_5 float not null,
    pm10 float not null,
    nh3 float not null,
    recorded_at timestamp with time zone default current_timestamp,
    zone_id integer not null,
    api_id integer not null,

    constraint air_quality_zone_id_fkey foreign key (zone_id) references weatherapp_schema.zone(id),
    constraint air_quality_api_id_fkey foreign key (api_id) references weatherapp_schema.api(id)
);
CREATE INDEX idx_air_quality_recorded_at ON weatherapp_schema.air_quality(recorded_at);