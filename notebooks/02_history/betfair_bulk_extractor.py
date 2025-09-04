import json
import time
from pathlib import Path

import pandas as pd

from whatstheodds.history.bulk_extractor import BulkOddsExtractor
from whatstheodds.history.bulkdownloader import BetfairBulkDownloader

extractor = BulkOddsExtractor(
    downloads_dir=Path(
        "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/"
    ),
    n_processes=2,  # Use 6 CPU cores
    batch_size=30,  # Process 50 matches at a time
)

# Extract all odds
summary = extractor.extract_from_downloads(
    phase1_json=Path("/home/james/bet_project/football_data/test/threaded_test.json"),
    output_csv=Path("/home/james/bet_project/football_data/test/all_odds.csv"),
    download_summary=Path(
        "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/download_summary.json"
    ),
    filter_complete_only=False,
    resume=True,
)


print(summary)

print(json.dumps(summary, indent=6))


"""
note book sections
"""

df = pd.read_csv("/home/james/bet_project/football_data/test/all_odds.csv")
df.tail()
df.columns
