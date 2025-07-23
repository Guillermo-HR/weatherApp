"""
Pipeline for extracting, transforming, and loading weather and air quality data.
Limits for Mexico City are:
* North: 19.60
* South: 19.05
* East: -98.95
* West: -99.35
Grid size is 0.05 degrees.
"""

from datetime import datetime, timezone 

from extract import Extract
from config.secrets import get_secrets
from config.arguments import get_args

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

def get_extractors(*api_data)-> dict:
    extractors = {}
    for api in api_data:
        api_name = api["api_name"]
        extractors[api_name] = Extract(
            api_name=api_name,
            api_key=api["api_key"],
            constant_params=api["constant_params"],
            search_params=api["search_params"],
            api_base_url=api["api_base_url"]
        )
    return extractors

def extract(data_coordinates:dict, extractors: dict, grid_size: float)-> dict:
    data = {}
    for name, extractor in extractors.items():
        data[name] = []

    for lat in data_coordinates["lat"]:
        for lon in data_coordinates["lon"]:
            for name, extractor in extractors.items():
                dt = datetime.now(timezone.utc).timestamp()
                response = extractor.get_data(lat, lon)
                if response["status"] == "failed":
                    continue
                data[name].append({
                    "lat": lat,
                    "lon": lon,
                    "grid_size": grid_size,
                    "data": response["data"],
                    "timestamp": dt
                })

    return data

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
    extractors = get_extractors(*api_data)
    data_coordinates = get_coordinates_mesh(
        north=app_args.max_lat,
        south=app_args.min_lat,
        east=app_args.max_lon,
        west=app_args.min_lon,
        grid_size=app_args.grid_size
    )

    data = extract(data_coordinates, extractors, app_args.grid_size)
    pass

if __name__ == "__main__":
    main()