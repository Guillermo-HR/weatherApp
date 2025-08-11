import requests
import logging
from typing import Dict

class Extract:
    def __init__(self, logger: logging.Logger, api_name:str, api_key:str, constant_params:str, 
                 search_params:str, api_base_url:str,):
        self.api_name = api_name
        self.api_key = api_key
        self.api_constant_params = constant_params
        self.api_search_params = search_params
        self.api_base_url = api_base_url
        self.logger = logger

    def validate_coordinates(self, latitude: float, longitude: float) -> bool:
        if not isinstance(latitude, (float, int)) or not isinstance(longitude, (float, int)):
            self.logger.error("Invalid coordinate type: latitude=%f, longitude=%f", 
                              latitude, longitude)
            return False
        
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            self.logger.error("Coordinates out of bounds: latitude=%f, longitude=%f", 
                              latitude, longitude)
            return False

        return True

    def get_data(self, latitude: float, longitude: float)->Dict:
        if not self.validate_coordinates(latitude, longitude):
            return {"status": "failed"}
        
        url = self.api_base_url 
        url += self.api_search_params.format(latitude = latitude, longitude = longitude)
        url += self.api_constant_params + self.api_key
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return {"status": "success", "data": data}
        except requests.exceptions.RequestException as e:
            self.logger.error(
                "API request failed for (%s) using (%s): %s",
                self.api_name,
                f"****{self.api_key[-4:]}",
                f"{e}".replace(self.api_key, f"****{self.api_key[-4:]}")
            )
            return {"status": "failed"}