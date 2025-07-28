"""
Pipeline for extracting, transforming, and loading weather and air quality data.
"""

from datetime import datetime, timezone 
import logging

from Extract import Extract
from transform.OpenWeatherAirQualityTransformer import OpenWeatherAirQualityTransformer
from transform.OpenWeatherWeatherTransformer import OpenWeatherWeatherTransformer
from load.OpenWeatherAirQualityLoader import OpenWeatherAirQualityLoader
from load.OpenWeatherWeatherLoader import OpenWeatherWeatherLoader
from config.secrets import get_secrets
from config.arguments import get_args

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_coordinates_mesh(north: float, south: float, east: float, west: float, grid_size: float) -> dict:
    coordinates = {
        "lat": [],
        "lon": []
    }

    lat = round(north, 3)
    while lat >= south:
        coordinates["lat"].append(lat)
        lat = round(lat - grid_size, 3)

    lon = round(west, 3)
    while lon <= east:
        coordinates["lon"].append(lon)
        lon = round(lon + grid_size, 3)

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

def get_transformers(*api_data:dict) -> dict:
    transformers = {}
    for api in api_data:
        api_name = api.get("api_name")
        if api_name == "Open weather air quality":
            transformers[api_name] = OpenWeatherAirQualityTransformer()
        elif api_name == "Open weather weather":
            transformers[api_name] = OpenWeatherWeatherTransformer()

    return transformers

def get_loaders(*api_data:dict) -> dict:
    loaders = {}
    for api in api_data:
        api_name = api.get("api_name")
        if api_name == "Open weather air quality":
            loaders[api_name] = OpenWeatherAirQualityLoader()
        elif api_name == "Open weather weather":
            loaders[api_name] = OpenWeatherWeatherLoader()

    return loaders

def extract(data_coordinates:dict, extractors: dict, grid_size: float)-> dict:
    data = {}
    successful = 0
    failed = 0

    for extractor_name in extractors:
        data[extractor_name] = []

    for lat in data_coordinates["lat"]:
        for lon in data_coordinates["lon"]:
            for extractor_name, extractor in extractors.items():
                timestamp = datetime.now(timezone.utc).timestamp()
                response = extractor.get_data(lat, lon)
                if response["status"] == "failed":
                    failed += 1
                    continue
                data[extractor_name].append({
                    "lat": lat,
                    "lon": lon,
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

    logging.info(f"Transformation completed: {successful} success, {failed} failed")
    return transformed_data

def load(transformed_data: dict, loaders: dict) -> None:
    successful = 0
    failed = 0

    for source, data in transformed_data.items():
        loader = loaders.get(source)
        prepared_data = loader.prepare_data(data) # type: ignore
        successful_loads, failed_loads = loader.load_data(prepared_data) # type: ignore
        successful += successful_loads
        failed += failed_loads

    logging.info(f"Loading completed: {successful} success, {failed} failed")


def main():
    app_secrets = get_secrets()
    app_args = get_args()
    api_data = [{
        "api_name": "Open weather weather",
        "api_key": f'&appid={app_secrets["OPEN_WEATHER_API_KEY"]}',
        "constant_params": "&units=metric&lang=es",
        "search_params": "lat={lat}&lon={lon}",
        "api_base_url": "https://api.openweathermap.org/data/2.5/weather?",
    },
    {
        "api_name": "Open weather air quality",
        "api_key": f'&appid={app_secrets["OPEN_WEATHER_API_KEY"]}',
        "constant_params": "&lang=es",
        "search_params": "lat={lat}&lon={lon}",
        "api_base_url": "http://api.openweathermap.org/data/2.5/air_pollution?",
    }]
    data_coordinates = get_coordinates_mesh(
        north=app_args.max_lat,
        south=app_args.min_lat,
        east=app_args.max_lon,
        west=app_args.min_lon,
        grid_size=app_args.grid_size
    )
    extractors = get_extractors(*api_data)
    loaders = get_loaders(*api_data)
    transformers = get_transformers(*api_data)
    loaders = get_loaders(*api_data)
    raw_data = extract(data_coordinates, extractors, app_args.grid_size)
    transformed_data = transform(raw_data, transformers)
    load(transformed_data, loaders)

if __name__ == "__main__":
    main()