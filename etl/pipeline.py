"""
Pipeline for extracting, transforming, and loading weather and air quality data.
"""

from datetime import datetime, timezone 
import logging
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Dict

from Extract import Extract
from transform.AirQualityTransformer import AirQualityTransformer
from transform.WeatherTransformer import WeatherTransformer
from Load import Load
from config.secrets import get_secrets
from config.arguments import get_args
from config.logger import setup_logger

def get_engine(database_user: str, database_password: str, 
               database_host: str, database_port: int, database_name: str):
    url = (
        f'postgresql+psycopg2://{database_user}:{database_password}'
        f'@{database_host}:{database_port}/{database_name}'
    )
    engine = create_engine(url)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
    except Exception as e:
        logger.critical(f"Failed to connect to the database: {e}")
        return None

    logger.info("Database connection established successfully.")
    return engine

def get_coordinates_mesh(max_latitude: float, min_latitude: float, max_longitude: float, 
                         min_longitude: float, grid_size: float) -> Dict:
    coordinates = {
        "latitude": [],
        "longitude": []
    }

    latitude = round(max_latitude, 5)
    while latitude > min_latitude:
        coordinates["latitude"].append(latitude)
        latitude = round(latitude - grid_size, 5)

    longitude = round(max_longitude, 5)
    while longitude > min_longitude:
        coordinates["longitude"].append(longitude)
        longitude = round(longitude - grid_size, 5)

    return coordinates

def get_zone_id(data_coordinates: Dict, engine) -> pd.DataFrame:
    latitude = [lat for lat in data_coordinates["latitude"]
                for lon in range(len(data_coordinates["longitude"]))]
    longitude = data_coordinates["longitude"] * len(data_coordinates["latitude"])

    query = f"""
        SELECT id, latitude, longitude
        FROM zone
        WHERE latitude IN ({','.join(map(str, latitude))})
        AND longitude IN ({','.join(map(str, longitude))});
    """

    try:
        zone_ids = pd.read_sql(query, engine)
    except Exception as e:
        logger.critical(f"Failed to retrieve zone IDs from the database: {e}")
        return None # type: ignore
    return zone_ids

def get_extractors(required_apis: Dict) -> Dict:
    api_data = {
        "OPEN_WEATHER_WEATHER": {
            "api_name": "Open Weather Weather",
            "api_key": f'&appid={{api_key}}',
            "constant_params": "&units=metric&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "https://api.openweathermap.org/data/2.5/weather?"
        },
        "OPEN_WEATHER_AIR_QUALITY": {
            "api_name": "Open Weather Air Quality",
            "api_key": f'&appid={{api_key}}',
            "constant_params": "&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "http://api.openweathermap.org/data/2.5/air_pollution?"
        }
    }
    
    extractors = {}
    for api_name, api_key in required_apis.items():
        if api_name not in api_data:
            logger.critical(f"API '{api_name}' is not supported.")
            return {}
        extractors[api_name] = Extract(
            logger = logger,
            api_name = api_data[api_name]["api_name"],
            api_key = api_data[api_name]["api_key"].format(api_key = api_key),
            constant_params = api_data[api_name]["constant_params"],
            search_params = api_data[api_name]["search_params"],
            api_base_url = api_data[api_name]["api_base_url"]
        )

    return extractors

def get_transformer(zone_ids: pd.DataFrame, target_table: str):
    transformers = ["weather", "air_quality"]
    if target_table not in transformers:
        logger.critical(f"Target table '{target_table}' is not supported.")
        return None
    
    if target_table == "weather":
        return WeatherTransformer(logger, zone_ids)
    elif target_table == "air_quality":
        return AirQualityTransformer(logger, zone_ids)

def add_missing_coordinates(data_coordinates: Dict, engine) -> int:
    latitude = [lat for lat in data_coordinates["latitude"] 
                     for lon in range(len(data_coordinates["longitude"]))]
    longitude = data_coordinates["longitude"] * len(data_coordinates["latitude"])
    
    candidate_coordinates = pd.DataFrame({
        "latitude": latitude,
        "longitude": longitude
    })

    lat_str = ','.join(map(str, latitude))
    lon_str = ','.join(map(str, longitude))
    query = f"""
        SELECT latitude, longitude
        FROM zone
        WHERE latitude IN ({lat_str})
        AND longitude IN ({lon_str});
    """
    try:
        existing_coordinates = pd.read_sql(query, engine)
    except Exception as e:
        logger.critical(f"Failed to retrieve existing coordinates from the database: {e}")
        return -1

    merge_coordinates = candidate_coordinates.merge(existing_coordinates, 
                                                    on = ["latitude", "longitude"],
                                                    how = "left", indicator = True)
    missing_coordinates = merge_coordinates[
        merge_coordinates["_merge"] == "left_only"
        ].drop(columns = ["_merge"])

    if not missing_coordinates.empty:
        try:
            missing_coordinates.to_sql("zone", engine, if_exists = "append", index = False)
        except Exception as e:
            logger.critical(f"Failed to insert missing coordinates into the database: {e}")
            return -1
    return 0

def unify_data(transformed_data: list, columns: list) -> Dict:
    unified_data = {}
    unified_data = {key: [] for key in columns}
    
    for record in transformed_data:
        if not all(key in record for key in columns):
            logger.error("Transformed data record is missing required keys.")
            continue
        for key in columns:
            unified_data[key].append(record[key])
    
    return unified_data

def extract(data_coordinates: Dict, extractors: Dict, grid_size: float) -> list:
    raw_data = []
    successful = 0
    failed = 0

    for latitude in data_coordinates["latitude"]:
        for longitude in data_coordinates["longitude"]:
            timestamp = datetime.now(timezone.utc).timestamp()
            record = {}
            record["latitude"] = latitude
            record["longitude"] = longitude
            record["grid_size"] = grid_size
            record["timestamp"] = timestamp
            record["data"] = {}
            for extractor_name, extractor in extractors.items():
                response = extractor.get_data(latitude, longitude)
                if response["status"] == "failed":
                    failed += 1
                    continue
                record["data"][extractor_name] = response["data"]
                successful += 1
            if len(record["data"]) != len(extractors):
               continue
            raw_data.append(record)

    logger.info(f"Extraction completed: {successful} success, {failed} failed")
    return raw_data

def transform(raw_data: list, transformer) -> Dict:
    transformed_data = []
    successful = 0
    failed = 0

    for record in raw_data:
        transformed_record = transformer.transform(record)
        if len(transformed_record) == 0:
            failed += 1
            continue
        transformed_data.append(transformed_record)
        successful += 1

    unified_data = unify_data(transformed_data, transformer.columns)

    logger.info(f"Transformation completed: {successful} success, {failed} failed")
    return unified_data

def load(transformed_data: dict, table: str, loader: Load) -> int:
    table_names = ["weather", "air_quality"]
    if table not in table_names:
        logger.critical(f"Target table '{table}' is not supported for loading.")
        return -1

    successful_loads, failed_loads = loader.load_data(transformed_data, table)

    logger.info(f"Loading completed: {successful_loads} success, {failed_loads} failed")
    return 0 if failed_loads == 0 else -1

def main():
    logger.info("Starting ETL pipeline")
    app_args = get_args(logger)
    if app_args is None:
        logger.info("Argument parsing failed. Exiting.")
        return
    app_secrets = get_secrets(app_args.target_table, logger)
    if app_secrets is None:
        logger.info("Failed to retrieve secrets. Exiting.")
        return
    engine = get_engine(
        database_user = app_secrets["database"]["DATABASE_USER"],
        database_password = app_secrets["database"]["DATABASE_PASSWORD"],
        database_host = app_secrets["database"]["DATABASE_HOST"],
        database_port = app_secrets["database"]["DATABASE_PORT"],
        database_name = app_secrets["database"]["DATABASE_NAME"]
    )
    if engine is None:
        logger.info("Database connection failed. Exiting.")
        return
    
    data_coordinates = get_coordinates_mesh(
        max_latitude = app_args.max_latitude,
        min_latitude = app_args.min_latitude,
        max_longitude = app_args.max_longitude,
        min_longitude = app_args.min_longitude,
        grid_size = app_args.grid_size
    )

    if add_missing_coordinates(data_coordinates, engine) == -1:
        logger.info("Failed to add missing coordinates. Exiting.")
        return

    zone_ids = get_zone_id(data_coordinates, engine)
    if zone_ids is None:
        logger.info("Failed to retrieve zone IDs. Exiting.")
        return
    if zone_ids.empty:
        logger.info("No zone IDs found. Exiting.")
        return

    extractors = get_extractors(app_secrets["required_apis"])
    if not extractors:
        logger.info("No extractors available. Exiting.")
        return
    transformer = get_transformer(zone_ids, app_args.target_table)
    if transformer is None:
        logger.info("No transformer available for the target table. Exiting.")
        return
    loader = Load(logger, engine)

    raw_data = extract(data_coordinates, extractors, app_args.grid_size)
    if not raw_data:
        logger.info("No data extracted. Exiting.")
        return
    transformed_data = transform(raw_data, transformer)
    if not transformed_data:
        logger.info("No data transformed. Exiting.")
        return
    result = load(transformed_data, app_args.target_table, loader)
    if result == -1:
        logger.info("Loading data failed. Exiting.")
        return

if __name__ == "__main__":
    global logger
    logger = setup_logger()
    main()