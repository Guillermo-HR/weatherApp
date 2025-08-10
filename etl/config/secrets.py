from dotenv import load_dotenv
import os
from typing import Dict
import logging


def get_secrets(target_table: str, logger: logging.Logger) -> Dict:
    tables = {
        "weather": {
            "OPEN_WEATHER_WEATHER": "OPEN_WEATHER_API_KEY"
        },
        "air_quality": {
            "OPEN_WEATHER_AIR_QUALITY": "OPEN_WEATHER_API_KEY"
        }
    }

    if target_table not in tables:
        logger.critical(f"target_table '{target_table}' is not supported.")
        raise ValueError(f"target_table '{target_table}' is not supported.")
    
    load_dotenv()

    required_apis = {key: os.getenv(value) for key, value in tables[target_table].items()}
    if not all(required_apis.values()):
        logger.critical("One or more API keys are missing in the environment variables.")
        return None # type: ignore

    database = ["DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_HOST", "DATABASE_NAME", 
                "DATABASE_PORT"]
    database_keys = {key: os.getenv(key) for key in database}
    if not all(database_keys.values()):
        logger.critical("One or more database configuration variables are missing in the " \
        "environment variables.")
        return None # type: ignore
    
    logger.info("Successfully retrieved all required secrets from environment variables.")
    return {
        "required_apis": required_apis,
        "database": database_keys
    }