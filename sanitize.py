import argparse

import pandas as pd

parser = argparse.ArgumentParser(
    description="Sanitize CSV file by removing specific columns."
)
parser.add_argument("input_file", type=str, help="Path to the input CSV file.")

args = parser.parse_args()

filtered_cols = [
    "Donor Email",
    "Donor Name",
    "Donor Comment",
]
f = pd.read_csv(args.input_file, usecols=lambda x: x not in filtered_cols)
f.to_csv(args.input_file, index=False)
