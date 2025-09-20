import json
from pathlib import Path

from whatstheodds.history.bulk_extractor import BulkOddsExtractor

extractor = BulkOddsExtractor(
    downloads_dir=Path(
        "/home/james/bet_project/football/eng_20_25_processing/download_odds/"
    ),
    n_processes=8,
    batch_size=100,
)

summary = extractor.extract_from_downloads(
    phase1_json=Path(
        "/home/james/bet_project/football/eng_20_25_processing/bet_grabber_24_25.json"
    ),
    output_csv=Path("/home/james/bet_project/football/england_20_25/odds.csv"),
    download_summary=Path(
        "/home/james/bet_project/football/eng_20_25_processing/download_odds/download_summary.json"
    ),
    filter_complete_only=False,
    resume=True,
)
print(summary)

print(json.dumps(summary, indent=6))
