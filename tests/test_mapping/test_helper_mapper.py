import logging
from datetime import datetime, timedelta

import betfairlightweight
import pytest
from omegaconf import DictConfig, OmegaConf

from src.betfair.api_client import CONFIG, setup_betfair_api_client
from src.betfair.dataclasses import BetfairSearchResultMultiDate
from src.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from src.mappers.helper_match_mapper import HelperMapper


def test_basic():
    print()
    print("- - " * 50)
    helper = HelperMapper()

    t_date = datetime(2022, 8, 30)
    t_delta = timedelta(days=1)

    results: BetfairSearchResultMultiDate = helper.search_all_match_via_date(
        t_date, t_delta
    )

    download_results = helper.download_matches(results)

    print(download_results)
