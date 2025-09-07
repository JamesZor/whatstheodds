import json
import time
from pathlib import Path

from whatstheodds.history.bulk_downloader import BetfairBulkDownloader

downloader = BetfairBulkDownloader(max_workers=40, show_rate_limit_bar=True)

print("waiting 2 secs ..")
time.sleep(2)
print("running")

results = downloader.download_from_phase1_json(
    json_path=Path(
        "/home/james/bet_project/football/eng_20_25_processing/bet_grabber_24_25.json"
    ),
    output_dir=Path(
        "/home/james/bet_project/football/eng_20_25_processing/download_odds/"
    ),
    resume=True,
)

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
