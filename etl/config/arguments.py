import argparse
import logging

logger = logging.getLogger(__name__)

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_latitude", type = float, default = 19.50, required = True)
    parser.add_argument("--min_latitude", type = float, default = 19.29, required = True)
    parser.add_argument("--max_longitude", type = float, default = -99.13, required = True)
    parser.add_argument("--min_longitude", type = float, default = -99.20, required = True)
    parser.add_argument("--grid_size", type = float, default = 0.02, required = True)
    parser.add_argument("--target_table", type = str, choices = ["weather", "air_quality"]
                        , default = "weather", required = True)

    # Validate arguments
    args = parser.parse_args()

    if args.max_latitude <= args.min_latitude:
        logger.error("max_latitude must be greater than min_latitude")
        raise ValueError("max_latitude must be greater than min_latitude")
    if args.max_longitude <= args.min_longitude:
        logger.error("max_longitude must be greater than min_longitude")
        raise ValueError("max_longitude must be greater than min_longitude")
    if args.grid_size <= 0:
        logger.error("grid_size must be a positive number")
        raise ValueError("grid_size must be a positive number")
    if args.grid_size < 0.00001:
        logger.error("grid_size must be at least 0.00001")
        raise ValueError("grid_size must be at least 0.00001")
    if args.target_table is None:
        logger.error("target_table must be specified")
        raise ValueError("target_table must be specified")
    if args.max_latitude > 90 or args.min_latitude < -90:
        logger.error("Latitude values must be between -90 and 90")
        raise ValueError("Latitude values must be between -90 and 90")
    if args.max_longitude > 180 or args.min_longitude < -180:
        logger.error("Longitude values must be between -180 and 180")
        raise ValueError("Longitude values must be between -180 and 180")

    return args

get_args()