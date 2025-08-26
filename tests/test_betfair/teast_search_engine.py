import logging
from datetime import datetime
from pathlib import Path

import betfairlightweight
import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.api_client import CONFIG, setup_betfair_api_client
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.pipeline.pipeline_coordinator import PipelineCoordinator


# @pytest.mark.skip("moved on")
def test_basic_setup():
    print()
    print("- - " * 50)

    bse = BetfairSearchEngine()
    print(OmegaConf.to_yaml(bse.cfg))

    test_search = BetfairSearchRequest(
        sofa_match_id=123456,
        home="Liverpool",
        away="Newcastle",
        date=datetime(2022, 8, 30),
        country="GB",
    )

    results = bse.search_match(test_search)

    print(results)
    print(len(results))
    print(len(bse.cfg.betfair_football.markets))


# @pytest.mark.skip("moved on")
def test_market_search():
    print()
    print("- - " * 50)

    bse = BetfairSearchEngine()
    print(OmegaConf.to_yaml(bse.cfg))

    test_search = BetfairSearchRequest(
        sofa_match_id=123456,
        home="Liverpool",
        away="Newcastle",
        date=datetime(2022, 8, 30),
        country="GB",
    )
    # market_type = "MATCH_ODDS"
    # results = bse.search_strategies._search_per_market_type(test_search, market_type)

    results = bse.search_main(test_search)

    for key, value in results.valid_markets.items():
        print(value)

    print("=" * 20)

    for key, value in results.missing_markets.items():
        print(value)


def test_market_search_from_df():
    print()
    print("- - " * 50)
    match_file_path = Path(
        "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
    )
    mm = MatchMapper()
    match_df = pd.read_csv(match_file_path)
    # Convert date column once
    match_df["match_date"] = pd.to_datetime(match_df["match_date"])
    bse = BetfairSearchEngine()
    print(OmegaConf.to_yaml(bse.cfg))

    test_search = BetfairSearchRequest(
        sofa_match_id=123456,
        home="Liverpool",
        away="Newcastle",
        date=datetime(2022, 8, 30),
        country="GB",
    )
    # market_type = "MATCH_ODDS"
    # results = bse.search_strategies._search_per_market_type(test_search, market_type)

    results = bse.search_main(test_search)

    for key, value in results.valid_markets.items():
        print(value)

    print("=" * 20)

    for key, value in results.missing_markets.items():
        print(value)
