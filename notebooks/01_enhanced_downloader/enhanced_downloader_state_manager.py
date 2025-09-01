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
from whatstheodds.pipeline.pipeline_coordinator_enhanced import PipelineCoordinator
from whatstheodds.processor.dataframe_processor import DataFrameProcessor
from whatstheodds.processor.state_manager import MatchState, ProcessingStateManager
from whatstheodds.storage.hybrid_storage import HybridStorage  # Chunk 3

##################################################
#
##################################################
"""
common parts for the set up 
"""
match_file_path = Path(
    "/home/james/bet_project/football_data/scot_nostats/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])
# Since this is an older df - i use the home_team which is the slug
df["home_team"] = df["home_team_slug"]
df["away_team"] = df["away_team_slug"]


##################################################
# Chunk 1 and 2
##################################################
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

##################################################
##################################################
"""
test 3 
adding chunk 3 package of the hybrid storage but 
"""
test_path = Path("/home/james/bet_project/whatstheodds/data/test_a")
downloader = BetfairDownloader()  # Enhanced version
mapper = MatchMapper()
search_engine = BetfairSearchEngine()
state_mgr = ProcessingStateManager(test_path, "run_name_a")  # chunk 3 version

hybrid_storage = HybridStorage()


row1 = df[
    (df["home_team_slug"] == "celtic") & (df["away_team_slug"] == "rangers")
].iloc[3]
print(row1)
row1 = df.iloc[5]
print(row1)
search_request = mapper.map_match_from_row(row1)
print(search_request)
search_results = search_engine.search_main(search_request)  # type: ignore[arg-type]
print(search_results)


files, status = downloader.download_odds_with_tracking(search_results)
print(files)
print(status)

# 1. Download with tracking (Chunk 2)
# 2. Update state (Chunk 1)


sofa_id = row1.match_id
sofa_id
betfair_id = search_results.match_id
run_name = "run_a"
state_mgr.update_match_downloads(sofa_id, status)
state_mgr.save_state()
# chunk 2 - whats the difference between update_match_search and update_match_download
state_mgr.update_match_search(sofa_id, search_results.match_id, search_results)
# search_results is not serializable via json


# 3. Archive if successful (Chunk 3)
if state_mgr.get_match_state(sofa_id).is_fully_successful():
    archive_path = hybrid_storage.archive_match(
        match_id=betfair_id, run_name=run_name, cleanup_temp=True
    )
    state_mgr.update_archive_path(sofa_id, str(archive_path))

state_mgr.save_state()
state_mgr.load_state()

state_mgr.get_statistics()
state_mgr.get_searchable_matches()
"""
[(11393373,
  MatchState(sofa_id=11393373, betfair_id=None, search_result=None, markets={'HALF_TIME_SCORE': 'success', 'OVER_UNDER_05': 'success', 'BOTH_TEAMS_TO_SCORE': 'success'}, download_attempts=1, archive_path=None, last_updated='2025-09-01T21:45:26.799326', search_cached=False, search_error=None))]
"""


##################################################
# Chuck 4
##################################################


test_path = Path("/home/james/bet_project/whatstheodds/data/test_b")
downloader = BetfairDownloader()  # Enhanced version
mapper = MatchMapper()
search_engine = BetfairSearchEngine()
state_mgr = ProcessingStateManager(test_path, "run_name_a")  # chunk 3 version
hybrid_storage = HybridStorage()
coordinator = PipelineCoordinator(tmp_manager=hybrid_storage)


####

row1 = df.iloc[69]
print(row1)
result = coordinator.process_from_row_with_state(
    row=row1, state_manager=state_mgr, archive_on_success=True
)
# Smart retry using ALL enhancements
retry_results = coordinator.retry_failed_matches(
    state_manager=state_mgr  # Uses cached searches!
)
print(result)
