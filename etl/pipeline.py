"""
Pipeline for extracting, transforming, and loading weather and air quality data.
"""

from datetime import datetime, timezone 
import logging
from sqlalchemy import create_engine, text
import pandas as pd

from Extract import Extract
from transform.AirQualityTransformer import AirQualityTransformer
from transform.WeatherTransformer import WeatherTransformer
from Load import Load
from config.secrets import get_secrets
from config.arguments import get_args
from typing import Dict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        logger.error(f"Failed to connect to the database: {e}")
        return None

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

def get_zone_id(data_coordinates: Dict, grid_size: float, engine) -> pd.DataFrame:
    latitude = [lat for lat in data_coordinates["latitude"]
                for lon in range(len(data_coordinates["longitude"]))]
    longitude = data_coordinates["longitude"] * len(data_coordinates["latitude"])

    query = f"""
        SELECT id, latitude, longitude, grid_size
        FROM zone
        WHERE latitude IN ({','.join(map(str, latitude))})
        AND longitude IN ({','.join(map(str, longitude))})
        AND grid_size = {grid_size};
    """

    return pd.read_sql(query, engine)

def get_extractors(required_apis: Dict) -> Dict:
    api_data = {
        "OPEN_WEATHER_WEATHER": {
            "api_name": "Open Weather Weather",
            "constant_params": "&units=metric&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "https://api.openweathermap.org/data/2.5/weather?"
        },
        "OPEN_WEATHER_AIR_QUALITY": {
            "api_name": "Open Weather Air Quality",
            "constant_params": "&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "http://api.openweathermap.org/data/2.5/air_pollution?"
        }
    }
    
    extractors = {}
    for api_name, api_key in required_apis.items():
        if api_name not in api_data:
            logger.error(f"API '{api_name}' is not supported.")
            raise ValueError(f"API '{api_name}' is not supported.")
        extractors[api_name] = Extract(
            api_name = api_data[api_name]["api_name"],
            api_key = api_key,
            constant_params = api_data[api_name]["constant_params"],
            search_params = api_data[api_name]["search_params"],
            api_base_url = api_data[api_name]["api_base_url"]
        )

    return extractors

def get_transformer(zone_ids: pd.DataFrame, target_table: str):
    transformers = ["weather", "air_quality"]
    if target_table not in transformers:
        logger.error(f"Target table '{target_table}' is not supported.")
        raise ValueError(f"Target table '{target_table}' is not supported.")
    
    if target_table == "weather":
        return WeatherTransformer(zone_ids)
    elif target_table == "air_quality":
        return AirQualityTransformer(zone_ids)

def get_api_id(engine) -> pd.DataFrame:
    query = "SELECT id, name FROM api;"

    return pd.read_sql(query, engine)

def add_missing_coordinates(data_coordinates: Dict, grid_size: float, engine) -> None:
    latitude = [lat for lat in data_coordinates["latitude"] 
                     for lon in range(len(data_coordinates["longitude"]))]
    longitude = data_coordinates["longitude"] * len(data_coordinates["latitude"])
    
    candidate_coordinates = pd.DataFrame({
        "latitude": latitude,
        "longitude": longitude,
        "grid_size": [grid_size] * len(latitude)
    })

    lat_str = ','.join(map(str, latitude))
    lon_str = ','.join(map(str, longitude))
    query = f"""
        SELECT latitude, longitude, grid_size
        FROM zone
        WHERE latitude IN ({lat_str})
        AND longitude IN ({lon_str})
        AND grid_size = {grid_size};
    """
    existing_coordinates = pd.read_sql(query, engine)

    merge_coordinates = candidate_coordinates.merge(existing_coordinates, 
                                                    on = ["latitude", "longitude", "grid_size"],
                                                    how = "left", indicator = True)
    missing_coordinates = merge_coordinates[
        merge_coordinates["_merge"] == "left_only"
        ].drop(columns = ["_merge"])

    if not missing_coordinates.empty:
        missing_coordinates.to_sql("zone", engine, if_exists = "append", index = False)

def unify_data(transformed_data: dict) -> dict:
    unified_data = {}
    for source, data in transformed_data.items():
        if not data:
            continue
        unified_data[source] = {}
        for record in data:
            for key, value in record.items():
                if key not in unified_data[source]:
                    unified_data[source][key] = []
                unified_data[source][key].append(value)
    return unified_data

def extract(data_coordinates:dict, extractors: dict, grid_size: float)-> dict:
    data = {}
    successful = 0
    failed = 0

    for extractor_name in extractors:
        data[extractor_name] = []

    for latitude in data_coordinates["latitude"]:
        for longitude in data_coordinates["longitude"]:
            for extractor_name, extractor in extractors.items():
                timestamp = datetime.now(timezone.utc).timestamp()
                response = extractor.get_data(latitude, longitude)
                if response["status"] == "failed":
                    failed += 1
                    continue
                data[extractor_name].append({
                    "latitude": latitude,
                    "longitude": longitude,
                    "grid_size": grid_size,
                    "data": response.get("data"),
                    "timestamp": timestamp
                })
                successful += 1

    logging.info(f"Extraction completed: {successful} success, {failed} failed")
    return data

def transform(raw_data: dict, transformers: dict) -> dict:
    transformed_data = {}
    successful = 0
    failed = 0

    for transformer_name in transformers:
        transformed_data[transformer_name] = []

    for source, data in raw_data.items():
        transformer = transformers.get(source)
        for record in data:
            if not transformer.validate_structure(record): # type: ignore
                failed += 1
                continue
            transformed_record = transformer.transform(record) # type: ignore
            if len(transformed_record) == 0:
                failed += 1
                continue
            transformed_data[source].append(transformed_record)
            successful += 1

    unified_data = unify_data(transformed_data)

    logging.info(f"Transformation completed: {successful} success, {failed} failed")
    return unified_data

def load(transformed_data: dict, loader: Load) -> None:
    table_names = {
        "Open Weather Weather": "weather",
        "Open Weather Air Quality": "air_quality"
    }
    successful = 0
    failed = 0

    for source, data in transformed_data.items():
        table = table_names.get(source)
        if not table:
            logging.warning(f"Unknown source: {source}")
            continue
        successful_loads, failed_loads = loader.load_data(data, table)
        successful += successful_loads
        failed += failed_loads

    logging.info(f"Loading completed: {successful} success, {failed} failed")

def main():
    app_args = get_args()
    app_secrets = get_secrets(app_args.target_table)
    engine = get_engine(
        database_user = app_secrets["database"]["DATABASE_USER"],
        database_password = app_secrets["database"]["DATABASE_PASSWORD"],
        database_host = app_secrets["database"]["DATABASE_HOST"],
        database_port = app_secrets["database"]["DATABASE_PORT"],
        database_name = app_secrets["database"]["DATABASE_NAME"]
    )
    if engine is None:
        logger.error("Exiting due to database connection failure.")
        return
    
    data_coordinates = get_coordinates_mesh(
        max_latitude = app_args.max_latitude,
        min_latitude = app_args.min_latitude,
        max_longitude = app_args.max_longitude,
        min_longitude = app_args.min_longitude,
        grid_size = app_args.grid_size
    )
    
    add_missing_coordinates(data_coordinates, app_args.grid_size, engine)
    zone_ids = get_zone_id(data_coordinates, app_args.grid_size, engine)

    extractors = get_extractors(app_secrets["required_apis"])
    transformer = get_transformer(zone_ids, app_args.target_table) #! last change
    loader = Load(engine)

    raw_data = extract(data_coordinates, extractors, app_args.grid_size)
    transformed_data = transform(raw_data, transformers)
    load(transformed_data, loader)

if __name__ == "__main__":
    main()