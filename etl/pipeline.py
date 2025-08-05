"""
Pipeline for extracting, transforming, and loading weather and air quality data.
"""

from datetime import datetime, timezone 
import logging
from sqlalchemy import create_engine
import pandas as pd

from Extract import Extract
from transform.OpenWeatherAirQualityTransformer import OpenWeatherAirQualityTransformer
from transform.OpenWeatherWeatherTransformer import OpenWeatherWeatherTransformer
from Load import Load
from config.secrets import get_secrets
from config.arguments import get_args

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_engine(db_user:str, db_password:str, 
               db_host:str, db_port:int, db_name:str):
    url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(url)

def get_coordinates_mesh(max_latitude: float, min_latitude: float, max_longitude: float, 
                         min_longitude: float, grid_size: float) -> dict:
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

def get_transformers(zone_ids: pd.DataFrame, api_ids: pd.DataFrame, *api_data:dict) -> dict:
    transformers = {}
    for api in api_data:
        api_name = api.get("api_name")
        api_id = api_ids[api_ids["name"] == api_name]["id"].values[0]
        if api_name == "Open Weather Air Quality":
            transformers[api_name] = OpenWeatherAirQualityTransformer(zone_ids, api_id)
        elif api_name == "Open Weather Weather":
            transformers[api_name] = OpenWeatherWeatherTransformer(zone_ids, api_id)

    return transformers

def get_zone_id(data_coordinates: dict, grid_size: float, engine) -> pd.DataFrame:
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

def get_api_id(engine) -> pd.DataFrame:
    query = "SELECT id, name FROM api;"

    return pd.read_sql(query, engine)

def add_missing_coordinates(data_coordinates:dict, grid_size:float, engine) -> None:
    latitude = [lat for lat in data_coordinates["latitude"] 
                     for lon in range(len(data_coordinates["longitude"]))]
    longitude = data_coordinates["longitude"] * len(data_coordinates["latitude"])
    
    candidates = pd.DataFrame({
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
    existing = pd.read_sql(query, engine)

    merged = candidates.merge(existing, on=["latitude", "longitude", "grid_size"], how="left", indicator=True)
    missing = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    if not missing.empty:
        missing.to_sql("zone", engine, if_exists="append", index=False)

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
    engine = get_engine(
        db_user=app_secrets["DB_USER"],
        db_password=app_secrets["DB_PASSWORD"],
        db_host=app_secrets["DB_HOST"],
        db_port=app_secrets["DB_PORT"],
        db_name=app_secrets["DB_NAME"]
    )
    data_coordinates = get_coordinates_mesh(
        max_latitude=app_args.max_latitude,
        min_latitude=app_args.min_latitude,
        max_longitude=app_args.max_longitude,
        min_longitude=app_args.min_longitude,
        grid_size=app_args.grid_size
    )
    
    add_missing_coordinates(data_coordinates, app_args.grid_size, engine)
    zone_ids = get_zone_id(data_coordinates, app_args.grid_size, engine)
    api_ids = get_api_id(engine)

    extractors = get_extractors(*api_data)
    transformers = get_transformers(zone_ids, api_ids, *api_data)
    loader = Load(engine)

    raw_data = extract(data_coordinates, extractors, app_args.grid_size)
    transformed_data = transform(raw_data, transformers)
    load(transformed_data, loader)

if __name__ == "__main__":
    main()