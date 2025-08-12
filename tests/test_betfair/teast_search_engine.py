import logging
from datetime import datetime

import betfairlightweight
import pytest
from omegaconf import DictConfig, OmegaConf

from src.betfair.api_client import CONFIG, setup_betfair_api_client
from src.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest


def test_basic_setup():
    print()
    print("- - " * 50)

    bse = BetfairSearchEngine()
    print(OmegaConf.to_yaml(bse.cfg))

    test_search = BetfairSearchRequest(
        sofa_match_id=123456,
        home="Liverpool",
        away="Newcastle",
        tournament="pl",
        date=datetime(2022, 8, 30),
        country="GB",
    )

    results = bse.search_match(test_search)

    print(results)
    print(len(results))
    print(len(bse.cfg.betfair_football.markets))
