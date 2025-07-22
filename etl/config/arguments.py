import argparse

def get_args()->argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_lon", type=float, default=19.60)
    parser.add_argument("--min_lon", type=float, default=19.05)
    parser.add_argument("--max_lat", type=float, default=-98.95)
    parser.add_argument("--min_lat", type=float, default=-99.35)
    parser.add_argument("--grid", type=float, default=0.05)
    return parser.parse_args()