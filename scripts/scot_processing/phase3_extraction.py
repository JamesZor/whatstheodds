import json
from pathlib import Path

from whatstheodds.history.bulk_extractor import BulkOddsExtractor
from whatstheodds.history.bulkdownloader import BetfairBulkDownloader

extractor = BulkOddsExtractor(
    downloads_dir=Path(
        "/home/james/bet_project/football/scot_20_25_processing/download_odds/"
    ),
    n_processes=8,  # Use 6 CPU cores
    batch_size=100,  # Process 50 matches at a time
)

# Extract all odds
summary = extractor.extract_from_downloads(
    phase1_json=Path(
        "/home/james/bet_project/football/scot_20_25_processing/scot_search_results.json"
    ),
    output_csv=Path("/home/james/bet_project/football/scot_20_25_f/odds.csv"),
    download_summary=Path(
        "/home/james/bet_project/football/scot_20_25_processing/download_odds/download_summary.json"
    ),
    filter_complete_only=False,
    resume=True,
)


print(summary)

print(json.dumps(summary, indent=6))
