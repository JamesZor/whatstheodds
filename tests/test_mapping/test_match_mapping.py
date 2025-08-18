import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from whatstheodds.mappers.match_mapper import MatchMapper


def test_basic_setup():
    print()
    print("= = " * 20)
    mm = MatchMapper()
    print
    print(mm.dir_mapping)
    print(json.dumps(mm.name_map, indent=3))
    name_one = "rangers"
    print(name_one)
    print(" to ")
    print(mm.get_mapped_name(name_one))


def test_match_map():
    print()
    print("= = " * 20)
    mapper = MatchMapper()
    search_request = mapper.map_match_with_tournament(
        sofa_match_id=12345,
        home_team_slug="celtic",
        away_team_slug="rangers",
        date=datetime(2025, 8, 15, 15, 0),
        tournament_id=54,
    )
    print(search_request)


def test_match_map_2():
    match_file_path = Path(
        "/home/james/bet_project/football/scot_test_mixed/football_data_mixed_matches.csv"
    )
    mm = MatchMapper()
    match_df = pd.read_csv(match_file_path)
    # Convert date column once
    match_df["match_date"] = pd.to_datetime(match_df["match_date"])

    for index, row in match_df.head(5).iterrows():
        print(f"{row['home_team_slug']} vs {row['away_team_slug']}")
        r = mm.map_match_from_row(row)
        print(f"Result: {r}")
