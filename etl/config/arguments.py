import argparse

def get_args()->argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_lat", type=float, default=19.55)
    parser.add_argument("--min_lat", type=float, default=19.40)
    parser.add_argument("--max_lon", type=float, default=-99.05)
    parser.add_argument("--min_lon", type=float, default=-99.20)
    parser.add_argument("--grid_size", type=float, default=0.05)
    
    return parser.parse_args()