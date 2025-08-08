import pandas as pd
import logging
from typing import Dict

class AirQualityTransformer:
    def __init__(self, zone_ids: pd.DataFrame):
        self.columns = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", 
                        "zone_id", "recorded_at"]
        self.zone_map = {
            (row['latitude'], row['longitude']): row['id']
            for _, row in zone_ids.iterrows()
        }
        self.metadata_keys = ["latitude", "longitude", "timestamp"]
        self.rules = {
            "recorded_at": {"type": (int), "min": 0},
            "zone_id": {"type": (int), "min": 0},
            "co": {"type": (float, int), "min": 0},
            "no": {"type": (float, int), "min": 0},
            "no2": {"type": (float, int), "min": 0},
            "o3": {"type": (float, int), "min": 0},
            "so2": {"type": (float, int), "min": 0},
            "pm2_5": {"type": (float, int), "min": 0},
            "pm10": {"type": (float, int), "min": 0},
            "nh3": {"type": (float, int), "min": 0},
        }
        self.logger = logging.getLogger(f"air_quality_transformer")

    def validate_structure(self, record:dict) -> bool:
        for key in self.metadata_keys:
            if key not in record:
                self.logger.error(f"Missing key in raw data: {key}")
                return False

        if "data" not in record or type(record["data"]) is not dict:
            self.logger.error("Invalid data structure.")
            return False

        return True

    def validate_data(self, data: Dict) -> bool:
        for field, rule in self.rules.items():
            value = data.get(field)
            if not isinstance(value, rule["type"]):
                self.logger.error(f"Invalid type for {field}, must be: {rule['type']}")
                return False
            
            if rule.get("min") is not None and value < rule["min"]:  # type: ignore
                self.logger.error(f"Value out of range for {field}: {value}")
                return False

            if rule.get("max") is not None and value > rule["max"]:  # type: ignore
                self.logger.error(f"Value out of range for {field}: {value}")
                return False
            
        return True

    def get_zone(self, latitude: float, longitude: float) -> int:
        if (latitude, longitude) in self.zone_map:
            zone_id = self.zone_map[(latitude, longitude)]
            return int(zone_id)
        else:
            self.logger.error(f"Zone not found for coordinates: ({latitude}, {longitude})")
            return -1

    def transform(self, record: Dict) -> Dict:
        if not self.validate_structure(record):
            return {}

        latitude = record["latitude"]
        longitude = record["longitude"]
        zone_id = self.get_zone(latitude, longitude)
        if zone_id == -1:
            return {}

        transformed_data = {
            "recorded_at": int(record["timestamp"]),
            "zone_id": zone_id,
            "co": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("co"),
            "no": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("no"),
            "no2": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("no2"),
            "o3": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("o3"),
            "so2": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("so2"),
            "pm2_5": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("pm2_5"),
            "pm10": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("pm10"),
            "nh3": record["data"]["OPEN_WEATHER_AIR_QUALITY"]["list"][0]["components"].get("nh3"),
        }

        if not self.validate_data(transformed_data):
            return {}
        
        return transformed_data