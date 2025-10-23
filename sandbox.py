from collections import Counter
import csv

from rnb import getDiff_RNB_from_file, rnb_get_most_recent

filename = "data/rnb_diff_2024-05-01.csv"
rnb_diff = getDiff_RNB_from_file(filename)
last_changes = rnb_get_most_recent(rnb_diff)

all_status = [row["status"] for row in last_changes]

print(Counter(all_status))
