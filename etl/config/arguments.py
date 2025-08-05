import argparse

def get_args()->argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_latitude", type=float, default=19.37)
    parser.add_argument("--min_latitude", type=float, default=19.29)
    parser.add_argument("--max_longitude", type=float, default=-99.13)
    parser.add_argument("--min_longitude", type=float, default=-99.20)
    parser.add_argument("--grid_size", type=float, default=0.02)
    
    return parser.parse_args()