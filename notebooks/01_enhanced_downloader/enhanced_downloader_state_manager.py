import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from whatstheodds.betfair.downloader_enhanced import (
    BetfairDownloader,
    BetfairDownloadError,
)
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.extractors import MatchExtractor
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor import DataFrameProcessor
from whatstheodds.processor.state_manager import MatchState, ProcessingStateManager

match_file_path = Path(
    "/home/james/bet_project/football_data/scot_nostats/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])
# Since this is an older df - i use the home_team which is the slug
df["home_team"] = df["home_team_slug"]
df["away_team"] = df["away_team_slug"]
"""
Test one 
Integration with Chunk 1 (State Manager):
"""

test_path = Path("/home/james/bet_project/whatstheodds/data/test")
state_mgr = ProcessingStateManager(test_path, "run_name")
downloader = BetfairDownloader()

row1 = df[
    (df["home_team_slug"] == "celtic") & (df["away_team_slug"] == "rangers")
].iloc[2]
print(row1)
mapper = MatchMapper()
search_engine = BetfairSearchEngine()

search_request = mapper.map_match_from_row(row1)
print(search_request)
search_results = search_engine.search_main(search_request)  # type: ignore[arg-type]
print(search_results)

"""
new bit, the tracking etc 
new method name 
"""
files, status = downloader.download_odds_with_tracking(search_results)
print(files)
print(status)

# Update state automatically
sofa_id = row1.match_id
state_mgr.update_match_downloads(sofa_id, status)
print(json.dumps(state_mgr.export_summary()))
print(state_mgr.get_retry_candidates())
print(state_mgr.get_statistics())
print(state_mgr.get_searchable_matches())
state_mgr.save_state()
"""
Out[31]: ✗ Failed 0.26s
[Error] TypeError: Object of type int64 is not JSON serializable
Traceback:
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
Cell In[31], line 1
----> 1 state_mgr.save_state()

File ~/bet_project/whatstheodds/src/whatstheodds/processor/state_manager.py:335, in ProcessingStateManager.save_state(self)
    333 self.output_dir.mkdir(parents=True, exist_ok=True)
    334 with open(self.state_file, "w") as f:
--> 335     json.dump(state_data, f, indent=2)
    337 logger.info(
    338     f"Saved state to {self.state_file} ({len(self.processing_state)} matches)"
    339 )
"""
state_mgr.state_file
