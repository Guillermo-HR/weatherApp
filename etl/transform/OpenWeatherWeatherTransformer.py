import logging

class OpenWeatherWeatherTransformer:
    def __init__(self):
        self.api_name = "Open weather weather"
        self.logger = logging.getLogger(f"{self.api_name}_extractor")

    def validate_structure(self, raw_data:dict) -> bool:
        metadata_keys = ["lat", "lon", "grid_size", "data", "timestamp"]
        for key in metadata_keys:
            if key not in raw_data:
                self.logger.error(f"Missing key in raw data: {key}")
                return False

        data_keys = ["temp", "humidity", "pressure"]
        for key in data_keys:
            if key not in raw_data["data"]["main"]:
                self.logger.error(f"Missing key in raw data: {key}")
                return False
            if not isinstance(raw_data['data']['main'][key], (int, float)):
                self.logger.error("Invalid data type for key %s: %s",
                                  key,
                                  raw_data["data"]["main"][key])
                return False
        return True

    def validate_data(self, raw_data:dict) -> bool:
        if not all([value for value in raw_data.values()]):
            self.logger.error("One or more required fields are missing in the raw data.")
            return False
        
        numeric_keys = ["temperature", "humidity", "pressure"]
        if not all([isinstance(raw_data.get(key), (int, float)) for key in numeric_keys]):
            self.logger.error("One or more values in the raw data are not numeric.")
            return False

        positive_keys = ["humidity", "pressure"]
        if not all([raw_data.get(key) > 0 for key in positive_keys]): # type: ignore
            self.logger.error("One or more values in the raw data are out of range.")
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

        if self.validate_data(transformed_data):
            return transformed_data
        
        return {}