import argparse

import pandas as pd

parser = argparse.ArgumentParser(
    description="Sanitize CSV file by removing specific columns."
)
parser.add_argument("input_file", type=str, help="Path to the input CSV file.")

args = parser.parse_args()

f = pd.read_csv(args.input_file, usecols=[0, 1, 3, 4])
f.to_csv(argparse.input_file, index=False)
