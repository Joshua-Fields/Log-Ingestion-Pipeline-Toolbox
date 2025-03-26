import re
import csv
from pathlib import Path

# Base directory is the 'Scripts' folder where the script lives
BASE_DIR = Path(__file__).resolve().parent

input_file = BASE_DIR / 'log' / 'PulseDescriptors.log'
output_file = BASE_DIR / 'csv' / 'PulseDescriptors.csv'

# Ensure the output directory exists
output_file.parent.mkdir(parents=True, exist_ok=True)

with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    for line in infile:
        line = line.strip()
        if not line:
            continue
        # Split by one or more spaces (handles uneven spacing)
        row = re.split(r'\s+', line)
        writer.writerow(row)

print(f"✅ Log converted to {output_file}")
