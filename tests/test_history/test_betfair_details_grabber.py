import json
import logging
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, call, patch

import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from whatstheodds.history.betfair_details import BetfairDetailsGrabber
from whatstheodds.mappers.match_mapper import MatchMapper

logging.basicConfig(level=logging.INFO)


#####
# CONFIG
TEST_DF_FILE_PATH = Path(
    "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
)
####


# Create the sofa dataframe
@pytest.fixture
def sofa_dataframe():
    df = pd.read_csv(TEST_DF_FILE_PATH)
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


class TestBetfairDetailsGrabber:

    def test_sofa_dataframe_load(self, sofa_dataframe):
        logging.info(f"Load the dataframe from:\n{str(TEST_DF_FILE_PATH)}.")
        logging.info(sofa_dataframe.head(5))

    def test_init_and_print_conf(self):
        betfair_details_grabber = BetfairDetailsGrabber()

        # check attributes
        assert isinstance(
            betfair_details_grabber, BetfairDetailsGrabber
        ), "Failed to init betfair grabber"
        assert isinstance(
            betfair_details_grabber.cfg, DictConfig
        ), f"cfg is not type Dcitconf : {type(betfair_details_grabber.cfg)}."
        assert isinstance(
            betfair_details_grabber.match_mapper, MatchMapper
        ), f"Failed to get match mapper, {type(betfair_details_grabber.match_mapper)}."

        logging.info("Displaying the current cfg loaded")
        logging.info(OmegaConf.to_yaml(betfair_details_grabber.cfg))

    def test_single_mapping_row(self, sofa_dataframe):
        betfair_details_grabber = BetfairDetailsGrabber()
        row_to_map: pd.Series = sofa_dataframe.sample(n=1).iloc[0]

        logging.info(f"row to be mapped:\n{row_to_map}\n{type(row_to_map)=}.")

        map_results: BetfairSearchRequest = betfair_details_grabber.map_match_from_row(
            row_to_map
        )
        logging.info(map_results)

        assert isinstance(
            map_results, BetfairSearchRequest
        ), f"Failed to get betfair search request: {type(map_results)}."

        assert (
            map_results.sofa_match_id == row_to_map["match_id"]
        ), "Match ids do not match."

        logging.info(json.dumps(map_results.to_dict(), indent=2))

    def test_single_betfair_search(self, sofa_dataframe):
        betfair_details_grabber = BetfairDetailsGrabber()

        row_to_map: pd.Series = sofa_dataframe.sample(n=1).iloc[0]
        logging.info(f"row to be mapped:\n{row_to_map}\n{type(row_to_map)=}.")

        map_results: BetfairSearchRequest = betfair_details_grabber.map_match_from_row(
            row_to_map
        )
        logging.info(map_results)
        assert isinstance(
            map_results, BetfairSearchRequest
        ), f"Failed to get betfair search request: {type(map_results)}."

        assert (
            map_results.sofa_match_id == row_to_map["match_id"]
        ), "Match ids do not match."

        logging.info(json.dumps(map_results.to_dict(), indent=2))

        search_result: BetfairSearchResult = betfair_details_grabber.search_match(
            map_results
        )

        logging.info(search_result)

        # logging.info(search_result.toJSON())
