from dotenv import load_dotenv
import os
from typing import Dict


def get_secrets()->Dict:
    load_dotenv()

    OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
    return {
        "OPEN_WEATHER_API_KEY": OPEN_WEATHER_API_KEY
    }