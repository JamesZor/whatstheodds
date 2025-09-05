import json
from pathlib import Path

import pandas as pd

from whatstheodds.history.betfair_details import BetfairDetailsGrabber

TEST_DF_FILE_PATH = Path(
    "/home/james/bet_project/football/england_20_25/football_data_mixed_matches.csv"
)

df = pd.read_csv(TEST_DF_FILE_PATH)
df["match_date"] = pd.to_datetime(df["match_date"])
df_reduced = df[df["season"] == "24/25"]

print(len(df_reduced))

# Initialize with directories
grabber = BetfairDetailsGrabber(
    input_dir=Path("/home/james/bet_project/football/eng_20_25_processing/"),
    output_dir=Path("/home/james/bet_project/football/eng_20_25_processing/"),
)

# First run
results = grabber.process_batch(
    matches_df=df_reduced,
    output_filename="bet_grabber_24_25.json",
    mode="new_only",
)

# Resume after interruption
# results = grabber.process_batch(
#     matches_df=matches_df, mode="new_and_incomplete"  # Only process incomplete
# )
# Get analysis
grabber.print_processing_summary(results)
summary = grabber.get_market_coverage_summary()
print(json.dumps(summary, indent=4))
