import pandas as pd
import logging

class OpenWeatherWeatherTransformer:
    def __init__(self, zone_ids: pd.DataFrame, api_id: int):
        self.api_name = "Open weather weather"
        self.zone_ids = zone_ids
        self.api_id = api_id
        self.metadata_keys = ["latitude", "longitude", "grid_size", "data", "timestamp"]
        self.data_keys = ["temp", "humidity", "pressure"]
        self.rules = {
            "recorded_at": {"type": (float, int), "min": 0},
            "temperature": {"type": (float, int), "min": -273.15},
            "humidity": {"type": (float, int), "min": 0, "max": 100},
            "pressure": {"type": (float, int), "min": 0, "max": 108000}
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
            if key not in raw_data["data"]["main"]:
                self.logger.error(f"Missing key in raw data: {key}")
                return False
            if not isinstance(raw_data['data']['main'][key], (int, float)):
                self.logger.error("Invalid data type for key %s: %s",
                                  key,
                                  raw_data["data"]["main"][key])
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
            "temperature": raw_data['data']['main'].get("temp"),
            "humidity": raw_data['data']['main'].get("humidity"),
            "pressure": raw_data['data']['main'].get("pressure")
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