import json
import time
from pathlib import Path

import pandas as pd

from whatstheodds.history.bulkdownloader import BetfairBulkDownloader

downloader = BetfairBulkDownloader(max_workers=25)

print("waiting 2 sec ... ")
time.sleep(2)
print("running")

results = downloader.download_from_phase1_json(
    json_path=Path("/home/james/bet_project/football_data/test/threaded_test.json"),
    output_dir=Path(
        "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/"
    ),
    resume=True,
    retry_failed=True,  # This tells it to retry previously failed files
)

print(results)
print(json.dumps(results, indent=4))
