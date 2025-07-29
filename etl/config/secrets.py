from dotenv import load_dotenv
import os
from typing import Dict


def get_secrets()->Dict:
    load_dotenv()

    OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
    return {
        "OPEN_WEATHER_API_KEY": OPEN_WEATHER_API_KEY,
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_PORT": int(os.getenv("DB_PORT", 5432))
    }