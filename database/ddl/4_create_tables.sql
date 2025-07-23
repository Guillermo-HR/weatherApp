create table zone (
    id serial primary key,
    north float not null,
    south float not null,
    east float not null,
    west float not null,
    gride_size float not null,

    constraint zone_north_south_check check (north > south),
    constraint zone_east_west_check check (east > west),
    constraint zone_gride_size_check check (gride_size > 0),
    constraint zone_north_south_east_west_grid_size_uq unique (
        north, south, east, west, gride_size)
);

create table api (
    id serial primary key,
    name text not null,

    constraint api_name_uq unique (name)
);

create table weather (
    id serial primary key,
    temperature double precision not null,
    humidity double precision not null,
    pressure double precision not null,
    recorded_at timestamp with time zone default current_timestamp,
    zone_id integer not null,
    api_id integer not null,

    constraint weather_zone_id_fkey foreign key (zone_id) references zone(id),
    constraint weather_api_id_fkey foreign key (api_id) references api(id)
);

create table air_quality (
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

    constraint air_quality_zone_id_fkey foreign key (zone_id) references zone(id),
    constraint air_quality_api_id_fkey foreign key (api_id) references api(id)
);