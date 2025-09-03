import json
from pathlib import Path

import pandas as pd

from whatstheodds.history.betfair_details import BetfairDetailsGrabber

TEST_DF_FILE_PATH = Path(
    "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
)

df = pd.read_csv(TEST_DF_FILE_PATH)
df["match_date"] = pd.to_datetime(df["match_date"])
df_reduced = df[900:905]


# Initialize with directories
grabber = BetfairDetailsGrabber(
    input_dir=Path("/home/james/bet_project/football_data/test"),
    output_dir=Path("/home/james/bet_project/football_data/test"),
)

# First run
results = grabber.process_batch(
    matches_df=df_reduced,
    output_filename="betfair_grab_test_results.json",
    mode="new_only",
)

# Resume after interruption
# results = grabber.process_batch(
#     matches_df=matches_df, mode="new_and_incomplete"  # Only process incomplete
# )
# Get analysis
summary = grabber.get_market_coverage_summary()
print(json.dumps(summary, indent=4))

# grabber.print_processing_summary(results)

incomplete_df = grabber.get_incomplete_matches()
print(incomplete_df)
