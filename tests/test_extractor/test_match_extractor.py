import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from extractors import MatchExtractor
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.dataclasses import BetfairSearchRequest
from whatstheodds.betfair.downloader import BetfairDownloader, BetfairDownloadError
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.pipeline.pipeline_coordinator import PipelineCoordinator
from whatstheodds.utils import load_config

"""
Index(['minutes', 'timestamp', 'over_0_5', 'under_0_5', 'over_3_5',
       'under_3_5', 'ht_over_1_5', 'ht_under_1_5', 'over_1_5', 'under_1_5',
       'ht_over_2_5', 'ht_under_2_5', 'ht_over_0_5', 'ht_under_0_5', 'home',
       'away', 'draw', 'ht_home', 'ht_away', 'ht_draw', 'seconds_to_kickoff',
       'home_prob', 'away_prob', 'draw_prob', 'overround', 'home_prob_norm',
       'away_prob_norm', 'draw_prob_norm', 'over_0_5_pct_change',
       'over_0_5_abs_change', 'under_0_5_pct_change', 'under_0_5_abs_change',
       'over_3_5_pct_change', 'over_3_5_abs_change', 'under_3_5_pct_change',
       'under_3_5_abs_change', 'ht_over_1_5_pct_change',
       'ht_over_1_5_abs_change', 'ht_under_1_5_pct_change',
       'ht_under_1_5_abs_change', 'over_1_5_pct_change', 'over_1_5_abs_change',
       'under_1_5_pct_change', 'under_1_5_abs_change',
       'ht_over_2_5_pct_change', 'ht_over_2_5_abs_change',
       'ht_under_2_5_pct_change', 'ht_under_2_5_abs_change',
       'ht_over_0_5_pct_change', 'ht_over_0_5_abs_change',
       'ht_under_0_5_pct_change', 'ht_under_0_5_abs_change', 'home_pct_change',
       'home_abs_change', 'away_pct_change', 'away_abs_change',
       'draw_pct_change', 'draw_abs_change', 'ht_home_pct_change',
       'ht_home_abs_change', 'ht_away_pct_change', 'ht_away_abs_change',
       'ht_draw_pct_change', 'ht_draw_abs_change', 'overround_pct_change',
       'overround_abs_change'],
      dtype='object')
"""


@pytest.mark.skip("working on score odds")
def test_basic():
    # Load your config
    config = load_config()
    print(OmegaConf.to_yaml(config))

    # Create extractor
    extractor = MatchExtractor(config)

    # Process a match
    match_id = "31679991"
    result = extractor.extract_match(match_id)

    # Access the data
    odds_df: pd.dataframe = result["odds_data"]  # pandas dataframe with all odds
    match_info = result["match_info"]  # Dict with team names, kickoff time

    print(json.dumps(match_info, indent=6))
    print("-" * 20)

    check = [
        "minutes",
        "timestamp",
        "home",
        "away",
        "draw",
        "ht_home",
        "ht_away",
        "ht_draw",
    ]
    check2 = [
        "minutes",
        "home_prob",
        "away_prob",
        "draw_prob",
        "home_prob_norm",
        "away_prob_norm",
        "draw_prob_norm",
        "overround",
    ]

    print(odds_df[check].head(20))

    print()
    print("-" * 20)

    # print(odds_df[check2].head(20))
    print(odds_df.columns)


def test_score_odd_extractions():
    match_file_path = Path(
        "/home/james/bet_project/football/scot_test_mixed/football_data_mixed_matches.csv"
    )
    df = pd.read_csv(match_file_path)
    # Convert date column once
    df["match_date"] = pd.to_datetime(df["match_date"])

    row = (
        df[df["tournament_id"] == 54].sort_values("match_date", ascending=False).iloc[3]
    )

    print(row)
    cfg = load_config("configs", config_file="test_score_odds.yaml")
    pipeline = PipelineCoordinator(cfg=cfg)
    # print(OmegaConf.to_yaml(cfg=cfg))

    # pipe line config
    date_column: str = "match_date"
    match_id_column: str = "match_id"
    home_column: str = "home_team_slug"
    away_column: str = "away_team_slug"
    tournament_column: str = "tournament_id"

    search_request = pipeline._map_match_from_row(
        row=row,
        date_column=date_column,
        match_id_column=match_id_column,
        home_column=home_column,
        away_column=away_column,
        tournament_column=tournament_column,
    )

    print(search_request)

    search_result = pipeline._search_match(search_request)
    print(search_result)
    download_files = pipeline._download_match_data(search_result)
    print(download_files)
    extracted_data = pipeline._extract_odds_data(search_result.match_id)
    # print(extracted_data)
    odds_df = extracted_data["odds_data"]

    print(odds_df.head(20))
