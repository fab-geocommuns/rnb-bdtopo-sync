from collections import Counter
import csv

from rnb import getDiff_RNB_from_file, rnb_get_most_recent

filename = "data/diff_2025-06-01.csv"

with open(filename, "r") as f:
    reader = csv.DictReader(f)

    data = list(reader)

    print(data[0]["is_active"])
    print(type(data[0]["is_active"]))
