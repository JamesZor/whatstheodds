import json
import logging
from datetime import datetime

import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf

from extractors import MatchExtractor
from src.betfair.dataclasses import BetfairSearchRequest
from src.betfair.downloader import BetfairDownloader, BetfairDownloadError
from src.betfair.search_engine import BetfairSearchEngine
from src.utils import load_config

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
    odds_df: pd.DataFrame = result["odds_data"]  # Pandas DataFrame with all odds
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
