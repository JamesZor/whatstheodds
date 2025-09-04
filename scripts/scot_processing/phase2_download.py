import json
import time
from pathlib import Path

import pandas as pd

from whatstheodds.history.bulkdownloader import BetfairBulkDownloader

downloader = BetfairBulkDownloader(max_workers=30)

print("waiting 2 secs ..")
time.sleep(2)
print("running")

results = downloader.download_from_phase1_json(
    json_path=Path(
        "/home/james/bet_project/football/scot_20_25_processing/scot_search_results.json"
    ),
    output_dir=Path(
        "/home/james/bet_project/football/scot_20_25_processing/download_odds/"
    ),
    resume=True,
    retry_failed=True,
)
