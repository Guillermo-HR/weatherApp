import argparse
import logging

def get_args(logger: logging.Logger) -> argparse.Namespace:
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
        logger.critical("max_latitude must be greater than min_latitude")
        return None # type: ignore
    if args.max_longitude <= args.min_longitude:
        logger.critical("max_longitude must be greater than min_longitude")
        return None # type: ignore
    if args.grid_size <= 0:
        logger.critical("grid_size must be a positive number")
        return None # type: ignore
    if args.grid_size < 0.00001:
        logger.critical("grid_size must be at least 0.00001")
        return None # type: ignore
    if args.target_table is None:
        logger.critical("target_table must be specified")
        return None # type: ignore
    if args.max_latitude > 90 or args.min_latitude < -90:
        logger.critical("Latitude values must be between -90 and 90")
        return None # type: ignore
    if args.max_longitude > 180 or args.min_longitude < -180:
        logger.critical("Longitude values must be between -180 and 180")
        return None # type: ignore

    logger.info(f"Arguments parsed successfully")
    return args