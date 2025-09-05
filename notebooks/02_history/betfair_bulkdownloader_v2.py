import json
import logging
import time
from pathlib import Path

import pandas as pd

from whatstheodds.history.bulk_downloader import BetfairBulkDownloader

downloader = BetfairBulkDownloader(max_workers=22, show_rate_limit_bar=True)
# logging.basicConfig()
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

print("waiting 2 sec ... ")
time.sleep(2)
print("running")

results = downloader.download_from_phase1_json(
    json_path=Path("/home/james/bet_project/football_data/test/threaded_test.json"),
    output_dir=Path(
        "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/"
    ),
    resume=True,
)

# print(results)
# print(json.dumps(results, indent=4))
# Add this snippet to get your summary
if results and "file_stats" in results:
    stats = results["file_stats"]

    total_processed = stats.get("processed_files_in_session", 0)
    downloaded = stats.get("downloaded_files", 0)
    skipped = stats.get("skipped_files", 0)
    failed = stats.get("failed_files", 0)
    invalid = stats.get("invalid_files", 0)

    print("\n--- Download Summary ---")
    print(f"Files Processed in this Session: {total_processed}")
    print(f"  ✅ Success: {downloaded}")
    print(f"  ⊙ Skipped (already existed): {skipped}")
    print(f"  ✗ Failed: {failed}")
    print(f"  ⚠ Invalid (HTML/corrupt): {invalid}")
    print("------------------------\n")

    # For a higher-level summary of matches:
    if "match_stats" in results:
        match_stats = results["match_stats"]
        print("--- Match Summary ---")
        print(f"Total Matches Processed: {match_stats.get('total_matches', 0)}")
        print(f"  ✅ Successful: {match_stats.get('successful', 0)}")
        print(f"  ~ Partial: {match_stats.get('partial', 0)}")
        print(f"  ✗ Failed: {match_stats.get('failed', 0)}")
        print("---------------------\n")

else:
    print("Could not generate summary. No results found.")
