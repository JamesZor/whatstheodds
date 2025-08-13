from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from src.mappers.match_mapper import MatchMapper

match_file_path = Path(
    "/home/james/bet_project/football/scot_test_mixed/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])


def test_one():
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
