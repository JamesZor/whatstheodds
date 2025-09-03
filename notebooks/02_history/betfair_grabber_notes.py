import json
from pathlib import Path

import pandas as pd

from whatstheodds.history.betfair_details import BetfairDetailsGrabber

TEST_DF_FILE_PATH = Path(
    "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
)

df = pd.read_csv(TEST_DF_FILE_PATH)
df["match_date"] = pd.to_datetime(df["match_date"])
df_reduced = df[900:1000]

grabber = BetfairDetailsGrabber(
    input_dir=Path("/home/james/bet_project/football_data/test"),
    output_dir=Path("/home/james/bet_project/football_data/test"),
)


"""
test one. 
old search method did a market ata time. 
Looking to get all files for the market in one
    - need to look that the return type for this, will be different from that of the betfairresult 
    - structure 
"""
row = df.iloc[433]
print(row)
search_request = grabber.map_match_from_row(row)
print(search_request)
search_results_all = grabber.search_match_all(search_request)
print(search_results_all)
###########################


"""
implemented the change 
notes on running the new version, well checks 

old without threading
"""
results = grabber.process_batch(
    matches_df=df_reduced, output_filename="fast_results.json", mode="new_only"
)

grabber.print_processing_summary(results)
""" Processing matches: 100%|██████████| 5/5 [02:14<00:00, 26.93s/it]
============================================================
PROCESSING SUMMARY (Fast Mode)
============================================================

Match Statistics:
  Total processed: 5
  Complete (all 12 markets): 5
  Failed/Incomplete: 0
  Success rate: 100.0%

Markets searched (12):
  HALF_TIME, FIRST_HALF_GOALS_05, FIRST_HALF_GOALS_15, FIRST_HALF_GOALS_25
  HALF_TIME_SCORE, MATCH_ODDS, OVER_UNDER_05, OVER_UNDER_15
  OVER_UNDER_25, OVER_UNDER_35, CORRECT_SCORE, BOTH_TEAMS_TO_SCORE

Progress: ████████████████████████████████████████ 100.0%
============================================================
"""


#########################################################
#
#########################################################
"""
add threading into the class 
"""


results = grabber.process_batch(
    matches_df=df_reduced,
    output_filename="threaded_test_v2.json",
    mode="new_only",
    use_threading=True,
)

grabber.print_processing_summary(results)
