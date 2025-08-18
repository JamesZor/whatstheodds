import logging
from datetime import datetime, timedelta

import betfairlightweight
import pytest
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.api_client import CONFIG, setup_betfair_api_client
from whatstheodds.betfair.dataclasses import BetfairSearchResultMultiDate
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.mappers.helper_match_mapper import HelperMapper


@pytest.mark.skip()
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

    extract_results = helper.extract_event_details(download_results)

    team_name = helper.extract_team_names(extract_results)

    print(team_name)


@pytest.mark.skip()
def test_get_team_names():

    print()
    print("- - " * 50)
    t_date = datetime(2022, 8, 30)
    t_delta = timedelta(days=30)
    helper = HelperMapper()
    # Simple usage - just get team names
    team_names = helper.process_matches_for_teams(t_date, t_delta)
    print(f"Found teams: {sorted(team_names)}")


def test_get_team_names_from_cache():
    print()
    print("- - " * 50)
    helper = HelperMapper()

    # Extract team names from already downloaded files
    team_names = helper.extract_team_names_from_tmp()
    print(f"Found {len(team_names)} unique teams from cached files:")

    # Print teams in a nice format
    sorted_teams = sorted(team_names)
    for i, team in enumerate(sorted_teams, 1):
        print(f"{team}")

    print(f"\nTotal: {len(team_names)} teams")
