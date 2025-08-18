import json
import logging
from datetime import datetime

import pytest
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.dataclasses import BetfairSearchRequest
from whatstheodds.betfair.downloader import BetfairDownloader, BetfairDownloadError
from whatstheodds.betfair.search_engine import BetfairSearchEngine


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
    # market_type = "MATCH_ODDS"
    # results = bse.search_strategies._search_per_market_type(test_search, market_type)

    search_result = bse.search_main(test_search)

    print("downloading")

    downloader = BetfairDownloader()
    # Download all valid markets
    try:
        downloaded_files = downloader.download_odds(search_result)
        print(f"Downloaded {len(downloaded_files)} files")

        # Verify downloads
        verification = downloader.verify_downloads(search_result)
        print(f"Verification: {verification}")

        # Get summary
        summary = downloader.get_download_summary(search_result)
        print(f"Success rate: {summary['download_success_rate']:.2%}")
        print(json.dumps(summary, indent=5))

    except BetfairDownloadError as e:
        print(f"Download failed: {e}")
