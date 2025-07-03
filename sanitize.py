import argparse

import pandas as pd

parser = argparse.ArgumentParser(
    description="Sanitize CSV file by removing specific columns."
)
parser.add_argument(
    "filenames", type=str, help="Path to the input CSV file.", nargs=argparse.REMAINDER
)
args = parser.parse_args()

filtered_cols = [
    "Donor Email",
    "Donor Name",
    "Donor Comment",
]

dono_files = args.filenames
print(f"Processing Files: {dono_files}")
for file in dono_files:
    f = pd.read_csv(file, usecols=lambda x: x not in filtered_cols)
    f.to_csv(file, index=False)
