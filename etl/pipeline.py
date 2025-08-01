"""
Pipeline for extracting, transforming, and loading weather and air quality data.
"""

from datetime import datetime, timezone 
import logging
from sqlalchemy import create_engine
import json

from Extract import Extract
from transform.OpenWeatherAirQualityTransformer import OpenWeatherAirQualityTransformer
from transform.OpenWeatherWeatherTransformer import OpenWeatherWeatherTransformer
from Load import Load
from config.secrets import get_secrets
from config.arguments import get_args

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_coordinates_mesh(max_latitude: float, min_latitude: float, max_longitude: float, 
                         min_longitude: float, grid_size: float) -> dict:
    coordinates = {
        "latitude": [],
        "longitude": []
    }

    latitude = round(max_latitude, 3)
    while latitude >= min_latitude:
        coordinates["latitude"].append(latitude)
        latitude = round(latitude - grid_size, 3)

    longitude = round(max_longitude, 3)
    while longitude >= min_longitude:
        coordinates["longitude"].append(longitude)
        longitude = round(longitude - grid_size, 3)

    return coordinates

def get_engine(db_user:str, db_password:str, 
               db_host:str, db_port:int, db_name:str):
    url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(url)

def get_extractors(*api_data:dict)-> dict:
    extractors = {}
    for api in api_data:
        api_name = api.get("api_name")
        api_key = api.get("api_key")
        constant_params = api.get("constant_params")
        search_params = api.get("search_params")
        api_base_url = api.get("api_base_url")
        if not all([api_name, api_key, constant_params, search_params, api_base_url]):
            logger.error(f"Missing parameters for extractor: {api_name}")
            continue
        extractors[api_name] = Extract(
            api_name=api_name, # type: ignore
            api_key=api_key, # type: ignore
            constant_params=constant_params, # type: ignore
            search_params=search_params, # type: ignore
            api_base_url=api_base_url # type: ignore
        )
    return extractors

def get_transformers(*api_data:dict) -> dict:
    transformers = {}
    for api in api_data:
        api_name = api.get("api_name")
        if api_name == "Open Weather Air Quality":
            transformers[api_name] = OpenWeatherAirQualityTransformer()
        elif api_name == "Open Weather Weather":
            transformers[api_name] = OpenWeatherWeatherTransformer()

    return transformers

def add_missing_coordinates(data_coordinates:dict, engine) -> None:
    
    pass

def create_unify_data(transformed_data: dict) -> dict:
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

    unified_data = create_unify_data(transformed_data)

    logging.info(f"Transformation completed: {successful} success, {failed} failed")
    return unified_data

def load(transformed_data: dict, loader: Load) -> None:
    successful = 0
    failed = 0

    for table, data in transformed_data.items():
        successful_loads, failed_loads = loader.load_data(data, table)
        successful += successful_loads
        failed += failed_loads

    logging.info(f"Loading completed: {successful} success, {failed} failed")

def main():
    app_secrets = get_secrets()
    app_args = get_args()
    api_data = [
        {
            "api_name": "Open Weather Weather",
            "api_key": f'&appid={app_secrets["OPEN_WEATHER_API_KEY"]}',
            "constant_params": "&units=metric&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "https://api.openweathermap.org/data/2.5/weather?",
        },
        {
            "api_name": "Open Weather Air Quality",
            "api_key": f'&appid={app_secrets["OPEN_WEATHER_API_KEY"]}',
            "constant_params": "&lang=es",
            "search_params": "lat={latitude}&lon={longitude}",
            "api_base_url": "http://api.openweathermap.org/data/2.5/air_pollution?",
        }
    ]
    data_coordinates = get_coordinates_mesh(
        max_latitude=app_args.max_latitude,
        min_latitude=app_args.min_latitude,
        max_longitude=app_args.max_longitude,
        min_longitude=app_args.min_longitude,
        grid_size=app_args.grid_size
    )
    engine = get_engine(
        db_user=app_secrets["DB_USER"],
        db_password=app_secrets["DB_PASSWORD"],
        db_host=app_secrets["DB_HOST"],
        db_port=app_secrets["DB_PORT"],
        db_name=app_secrets["DB_NAME"]
    )
    extractors = get_extractors(*api_data)
    transformers = get_transformers(*api_data)
    loader = Load(engine)

    add_missing_coordinates(data_coordinates, engine)

    raw_data = extract(data_coordinates, extractors, app_args.grid_size)
    with open("raw_data.json", "w") as f:
        json.dump(raw_data, f, indent=4)
    transformed_data = transform(raw_data, transformers)
    with open("transformed_data.json", "w") as f:
        json.dump(transformed_data, f, indent=4)
#    load(transformed_data, loader)

if __name__ == "__main__":
    main()