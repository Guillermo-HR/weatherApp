import logging

class OpenWeatherWeatherTransformer:
    def __init__(self):
        self.api_name = "Open weather weather"
        self.metadata_keys = ["lat", "lon", "grid_size", "data", "timestamp"]
        self.data_keys = ["temp", "humidity", "pressure"]
        self.rules = {
            "lat": {"type": (float, int), "min": -90, "max": 90},
            "lon": {"type": (float, int), "min": -180, "max": 180},
            "grid_size": {"type": (float, int), "min": 0.009},
            "timestamp": {"type": (float, int), "min": 0},
            "temperature": {"type": (float, int), "min": -273.15},
            "humidity": {"type": (float, int), "min": 0, "max": 100},
            "pressure": {"type": (float, int), "min": 0, "max": 108000}
        }
        self.logger = logging.getLogger(f"{self.api_name}_extractor")

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
            "lat": raw_data.get("lat"),
            "lon": raw_data.get("lon"),
            "grid_size": raw_data.get("grid_size"),
            "timestamp": raw_data.get("timestamp"),
            "temperature": raw_data['data']['main'].get("temp"),
            "humidity": raw_data['data']['main'].get("humidity"),
            "pressure": raw_data['data']['main'].get("pressure")
        }

        if not self.validate_data(transformed_data):
            return {}
        transformed_data["timestamp"] = int(transformed_data["timestamp"])
        return transformed_data