import pandas as pd
import logging

class OpenWeatherAirQualityTransformer:
    def __init__(self, zone_ids: pd.DataFrame, api_id: int):
        self.api_name = "Open weather air quality"
        self.zone_ids = zone_ids
        self.api_id = api_id
        self.metadata_keys = ["latitude", "longitude", "grid_size", "data", "timestamp"]
        self.data_keys = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]
        self.rules = {
            "recorded_at": {"type": (float, int), "min": 0},
            "co": {"type": (float, int), "min": 0},
            "no": {"type": (float, int), "min": 0},
            "no2": {"type": (float, int), "min": 0},
            "o3": {"type": (float, int), "min": 0},
            "so2": {"type": (float, int), "min": 0},
            "pm2_5": {"type": (float, int), "min": 0},
            "pm10": {"type": (float, int), "min": 0},
            "nh3": {"type": (float, int), "min": 0},
        }
        self.logger = logging.getLogger(f"{self.api_name}_transformer")

    def validate_structure(self, raw_data:dict) -> bool:
        for key in self.metadata_keys:
            if key not in raw_data:
                self.logger.error(f"Missing key in raw data: {key}")
                return False
            
        if type(raw_data.get("data")) is not dict:
            self.logger.error("Invalid data structure.")
            return False

        for key in self.data_keys:
            if key not in raw_data['data']['list'][0]["components"]:
                self.logger.error(f"Missing key in raw data: {key}")
                return False
            if not isinstance(raw_data['data']['list'][0]["components"][key], (int, float)):
                self.logger.error("Invalid data type for key %s: %s",
                                  key,
                                  raw_data['data']['list'][0]['components'][key])
                return False
        return True

    def validate_data(self, data: dict) -> bool:
        for field, rule in self.rules.items():
            value = data.get(field)
            if not isinstance(value, rule["type"]):
                self.logger.error(f"Invalid type for {field}: {type(value)}")
                return False
            
            if rule.get("min") is not None and value < rule["min"]:  # type: ignore
                self.logger.error(f"Value out of range for {field}: {value}")
                return False

            if rule.get("max") is not None and value > rule["max"]:  # type: ignore
                self.logger.error(f"Value out of range for {field}: {value}")
                return False
        return True

    def transform(self, raw_data:dict) -> dict:
        transformed_data = {
            "api_id": self.api_id,
            "recorded_at": raw_data.get("timestamp"),
            "co": raw_data["data"]["list"][0]["components"].get("co"),
            "no": raw_data["data"]["list"][0]["components"].get("no"),
            "no2": raw_data["data"]["list"][0]["components"].get("no2"),
            "o3": raw_data["data"]["list"][0]["components"].get("o3"),
            "so2": raw_data["data"]["list"][0]["components"].get("so2"),
            "pm2_5": raw_data["data"]["list"][0]["components"].get("pm2_5"),
            "pm10": raw_data["data"]["list"][0]["components"].get("pm10"),
            "nh3": raw_data["data"]["list"][0]["components"].get("nh3"),
        }

        if not self.validate_data(transformed_data):
            return {}
        
        latitude = raw_data.get("latitude")
        longitude = raw_data.get("longitude")
        grid_size = raw_data.get("grid_size")
        zone_id = self.zone_ids[
            (self.zone_ids["latitude"] == latitude) & 
            (self.zone_ids["longitude"] == longitude) &
            (self.zone_ids["grid_size"] == grid_size)
        ]["id"].values[0]
        zone_id = int(zone_id) 
        
        transformed_data["zone_id"] = zone_id
        transformed_data["recorded_at"] = int(transformed_data["recorded_at"])
        return transformed_data