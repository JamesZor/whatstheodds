from pathlib import Path

import pandas as pd

from whatstheodds.history.betfair_details import BetfairDetailsGrabber

### CONFIG
MATCHES_FILE_PATH = Path(
    "/home/james/bet_project/football/scot_20_25_f/football_data_mixed_matches.csv"
)
df = pd.read_csv(MATCHES_FILE_PATH)

df["match_date"] = pd.to_datetime(df["match_date"])

grabber = BetfairDetailsGrabber(
    input_dir=Path("/home/james/bet_project/football/scot_20_25_processing/"),
    output_dir=Path("/home/james/bet_project/football/scot_20_25_processing/"),
)


results = grabber.process_batch(
    matches_df=df, output_filename="scot_search_results.json", mode="new_and_failed"
)

grabber.print_processing_summary(results)
