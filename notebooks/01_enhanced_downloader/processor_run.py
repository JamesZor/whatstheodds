from datetime import datetime
from pathlib import Path

import pandas as pd

from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor_enhanced import DataFrameProcessor

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


matches_df = df[200:205]
print(matches_df)
mm = MatchMapper()

for idx, row in matches_df.iterrows():
    print(f"home: {row.home_team} v {row.away_team}")
    print(mm.map_match_from_row(row))


# 1. Simple processing with everything
processor = DataFrameProcessor()
results, files, state = processor.process_and_save_with_state(
    df=matches_df, run_name="production_run", archive_successful=True
)

# 2. If failures, easy retry
if state.get_statistics()["retry_candidates"] > 0:
    retry_results, updated = processor.retry_failed_from_state(
        state_file_path=state.state_file
    )

# 3. Generate report
report = processor.generate_failure_report(state.state_file)

print(report)


print(f"Success rate: {report['summary']['success_rate']}%")


"""


Failed to retrieve file list: DownloadListOfFiles 
Params: {'sport': 'Soccer', 'plan': 'Basic Plan', 'fromDay': '2', 'fromMonth': '9', 'fromYear': '2022', 'toDay': '4', 'toMonth': '9', 'toYear': '2022', 'eventName': 'St Johnstone v St Mirren', 'marketTypesCollection': ['OVER_UNDER_25'], 'countriesCollection': ['GB'], 'fileTypeCollection': ['M']} 
Exception: HTTPSConnectionPool(host='historicdata.betfair.com', port=443): Read timed out. (read timeout=16)
Error in match search: Failed to retrieve file list: DownloadListOfFiles 
Params: {'sport': 'Soccer', 'plan': 'Basic Plan', 'fromDay': '2', 'fromMonth': '9', 'fromYear': '2022', 'toDay': '4', 'toMonth': '9', 'toYear': '2022', 'eventName': 'St Johnstone v St Mirren', 'marketTypesCollection': ['OVER_UNDER_25'], 'countriesCollection': ['GB'], 'fileTypeCollection': ['M']} 
Exception: HTTPSConnectionPool(host='historicdata.betfair.com', port=443): Read timed out. (read timeout=16)
Match 10387468 failed at search_failed: Match not found on Betfair
Processing matches:  90%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████▌                 | 9/10 [04:02<00:30, 30.22s/match, success=0, failed=9, current=1.04e+7]Request allowed. Current count: 1/90
Request allowed. Current count: 2/90
Request allowed. Current count: 2/90
Request allowed. Current count: 3/90
Request allowed. Current count: 4/90
Request allowed. Current count: 5/90
Failed to retrieve file list: DownloadListOfFiles 
Params: {'sport': 'Soccer', 'plan': 'Basic Plan', 'fromDay': '3', 'fromMonth': '3', 'fromYear': '2023', 'toDay': '5', 'toMonth': '3', 'toYear': '2023', 'eventName': 'Livingston v Hibernian', 'marketTypesCollection': ['MATCH_ODDS'], 'countriesCollection': ['GB'], 'fileTypeCollection': ['M']} 
Exception: HTTPSConnectionPool(host='historicdata.betfair.com', port=443): Read timed out. (read timeout=16)
Error in match search: Failed to retrieve file list: DownloadListOfFiles 
Params: {'sport': 'Soccer', 'plan': 'Basic Plan', 'fromDay': '3', 'fromMonth': '3', 'fromYear': '2023', 'toDay': '5', 'toMonth': '3', 'toYear': '2023', 'eventName': 'Livingston v Hibernian', 'marketTypesCollection': ['MATCH_ODDS'], 'countriesCollection': ['GB'], 'fileTypeCollection': ['M']} 
Exception: HTTPSConnectionPool(host='historicdata.betfair.com', port=443): Read timed out. (read timeout=16)
Match 10387469 failed at search_failed: Match not found on Betfair
Unexpected error processing row 9: Object of type BetfairSearchSingleMarketResult is not JSON serializable
Processing matches: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 10/10 [04:31<00:00, 27.12s/match, success=0, failed=9, current=1.04e+7]
Traceback (most recent call last):
  File "/home/james/bet_project/whatstheodds/notebooks/01_enhanced_downloader/processor_run.py", line 31, in <module>
    results, files, state = processor.process_and_save_with_state(
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/james/bet_project/whatstheodds/src/whatstheodds/processor/dataframe_processor_enhanced.py", line 109, in process_and_save_with_state
    results = self.coordinator.process_dataframe_with_state(
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/james/bet_project/whatstheodds/src/whatstheodds/pipeline/pipeline_coordinator_enhanced.py", line 533, in process_dataframe_with_state
    state_manager.save_state()
  File "/home/james/bet_project/whatstheodds/src/whatstheodds/processor/state_manager.py", line 337, in save_state
    json.dump(state_data, f, indent=2)
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/__init__.py", line 179, in dump
    for chunk in iterable:
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 432, in _iterencode
    yield from _iterencode_dict(o, _current_indent_level)
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 406, in _iterencode_dict
    yield from chunks
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 406, in _iterencode_dict
    yield from chunks
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 406, in _iterencode_dict
    yield from chunks
  [Previous line repeated 1 more time]
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 439, in _iterencode
    o = _default(o)
        ^^^^^^^^^^^
  File "/home/james/miniconda3/envs/webscraper/lib/python3.11/json/encoder.py", line 180, in default
    raise TypeError(f'Object of type {o.__class__.__name__} '
TypeError: Object of type BetfairSearchSingleMarketResult is not JSON serializable
"""
