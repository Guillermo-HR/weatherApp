import argparse

def get_args()->argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_latitude", type=float, default=19.55)
    parser.add_argument("--min_latitude", type=float, default=19.50)
    parser.add_argument("--max_longitude", type=float, default=-99.05)
    parser.add_argument("--min_longitude", type=float, default=-99.10)
    parser.add_argument("--grid_size", type=float, default=0.05)
    
    return parser.parse_args()