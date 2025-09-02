import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor_enhanced import DataFrameProcessor

match_file_path = Path(
    "/home/james/bet_project/football_data/scot_20_25_f/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
df["match_date"] = pd.to_datetime(df["match_date"])


#
# 1. Simple processing with everything
processor = DataFrameProcessor()
results, files, state = processor.process_and_save_with_state(
    df=df, run_name="scot_f", archive_successful=True
)

# 2. If failures, easy retry
if state.get_statistics()["retry_candidates"] > 0:
    retry_results, updated = processor.retry_failed_from_state(
        state_file_path=state.state_file
    )

# 3. Generate report
report = processor.generate_failure_report(state.state_file)

print(json.dumps(report, indent=4))
