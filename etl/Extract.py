import requests
from datetime import datetime, timezone
import logging
from typing import Dict

class Extract:
    def __init__(self, api_name:str, api_key:str, constant_params:str, 
                 search_params:str, api_base_url:str,):
        self.api_name = api_name
        self.api_key = api_key
        self.api_constant_params = constant_params
        self.api_search_params = search_params
        self.api_base_url = api_base_url
        self.logger = logging.getLogger(f"{api_name}_extractor")

    def validate_coordinates(self, lat:float, long:float) -> bool:
        if not isinstance(lat, (float, int)) or not isinstance(long, (float, int)):
            self.logger.error("Invalid coordinate type: lat=%f, long=%f", lat, long)
            return False
        
        if not (-90 <= lat <= 90) or not (-180 <= long <= 180):
            self.logger.error("Coordinates out of bounds: lat=%f, long=%f", lat, long)
            return False

        return True

    def get_data(self, lat:float, long:float)->Dict:
        if not self.validate_coordinates(lat, long):
            return {"status": "failed"}
    
        url = self.api_base_url + self.api_search_params.format(lat=lat, lon=long)
        url += self.api_constant_params + self.api_key
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return {"status": "success", "data": data}
        except requests.exceptions.RequestException as e:
            timestamp = datetime.now(timezone.utc).timestamp()
            self.logger.error(
                "API request failed for (%s) using (%s) at (%f): %s",
                self.api_name,
                f"****{self.api_key[-4:]}",
                timestamp,
                f"{e}".replace(self.api_key, f"****{self.api_key[-4:]}")
            )
            return {"status": "failed"}