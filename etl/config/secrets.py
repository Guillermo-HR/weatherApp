from dotenv import load_dotenv
import os
from typing import Dict


def get_secrets()->Dict:
    load_dotenv()

    return {
        "OPEN_WEATHER_API_KEY": os.getenv("OPEN_WEATHER_API_KEY"),
        "DB_USER": os.getenv("DATABASE_USER"),
        "DB_PASSWORD": os.getenv("DATABASE_PASSWORD"),
        "DB_HOST": os.getenv("DATABASE_HOST"),
        "DB_NAME": os.getenv("DATABASE_NAME"),
        "DB_PORT": int(os.getenv("DATABASE_PORT", 5432))
    }