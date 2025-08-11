# HACK: - tmp location of the DataClasses here
# Move to src.core
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import betfairlightweight
from omegaconf import DictConfig

from .api_client import setup_betfair_api_client


########################################
# HACK: dataclass
########################################
@dataclass
class BetfairSearchRequest:
    sofa_match_id: int
    home_team: str
    away_team: str
    tournament: Optional[str]
    match_data: datetime
    country: str


@dataclass
class BetfairSearchResult:
    match_id: Optional[str]
    strategy_used: str


#    confidence: float (0-1)
# search_attempts: List[SearchAttempt]


########################################
# TODO: search strategies
########################################
class BaseSearchStrategy(ABC):

    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        self.cfg: DictConfig = cfg
        self.client: betfairlightweight.APIClient = client

    @abstractmethod
    def search(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        pass


class ExactDateTeamSearch(BaseSearchStrategy):
    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        super().__init__(cfg=cfg, client=client)

    def search(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:



########################################
# Main class
########################################
class BetfairSearchEngine:
    """
    Searches for matches on Betfair with multiple strategies
    """

    def __init__(self, api_client: betfairlightweight.APIClient):
        self.client = api_client
        self.search_cache = {}
        self.search_strategies = [
            ExactDateTeamSearch(),
            ExpandedDateSearch(days=2),
            FuzzyTeamNameSearch(threshold=0.85),
            ManualMappingSearch(),
        ]

    def search_match(
        self, params: BetfairSearchParams, use_cache: bool = True
    ) -> SearchResult:
        """
        Search for a match using multiple strategies

        Returns:
            SearchResult with:
                - match_id: Optional[str]
                - confidence: float (0-1)
                - strategy_used: str
                - search_attempts: List[SearchAttempt]
        """

    def bulk_search(self, params_list: List[BetfairSearchParams]) -> List[SearchResult]:
        """
        Efficiently search for multiple matches
        Uses caching and rate limiting
        """
