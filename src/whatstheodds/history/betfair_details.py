import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import betfairlightweight
import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)

# CONFIGS
CONFIG_DIR: str = "configs"
CONFIG_FILE: str = "history_test.yaml"

# Phase 1: Database → Your SearchEngine → Mapping Table CSV
# =============================================================================
# PHASE 1: CREATE MAPPING TABLE (Uses your existing search)
# =============================================================================


class BetfairDetailsGrabber:
    """
    Coordinates the entire pipeline from match row to extracted betfair match details
    """

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
        match_mapper: Optional[MatchMapper] = None,
        search_engine: Optional[BetfairSearchEngine] = None,
    ):
        self.cfg: DictConfig = load_config(config_dir=CONFIG_DIR, config_file=CONFIG_FILE) if cfg is None else cfg  # type: ignore[assignment]  # type: ignore[unused-ignore]

        # Initialize core components
        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )

        # Initialize pipeline components
        self.match_mapper: MatchMapper = (
            MatchMapper(cfg=self.cfg) if match_mapper is None else match_mapper
        )

        self.search_engine: BetfairSearchEngine = (
            BetfairSearchEngine(api_client=self.client, cfg=self.cfg)
            if search_engine is None
            else search_engine
        )

    def map_match_from_row(
        self,
        row: pd.Series,
    ) -> Optional[BetfairSearchRequest]:
        """
        Map a row from a df, to a Search Request
        """
        try:
            return self.match_mapper.map_match_from_row(row=row)
        except Exception as e:
            logger.error(f"Error in match mapping: {str(e)}")
            return None

    def search_match(
        self, search_request: BetfairSearchRequest
    ) -> Optional[BetfairSearchResult]:
        """Search for match using BetfairSearchEngine"""
        try:
            return self.search_engine.search_main(search_request)
        except Exception as e:
            logger.error(f"Error in match search: {str(e)}")
            return None

    def process_from_row(self, row: pd.Series) -> Optional[BetfairSearchResult]:

        search_request: BetfairSearchRequest = self.map_match_from_row(row)

        if search_request is None:
            # TODO:
            pass

        search_result: BetfairSearchResult = self.search_match(
            search_request=search_request
        )
