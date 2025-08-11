# HACK: - tmp location of the DataClasses here
# Move to src.core
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig

from src.utils import load_config

from .api_client import setup_betfair_api_client

logger = logging.getLogger(__name__)


########################################
# HACK: dataclass
########################################
@dataclass
class BetfairSearchRequest:
    sofa_match_id: int
    home: str
    away: str
    tournament: Optional[str]
    date: datetime
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

        # HACK:
        self.config = self.cfg.search.exact_date_team_search

    def get_football_event_name(self, search_request: BetfairSearchRequest) -> str:
        return f"{search_request.home} v {search_request.away}"

    @abstractmethod
    def search(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        pass


class ExactDateTeamSearch(BaseSearchStrategy):
    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        super().__init__(cfg=cfg, client=client)

    def get_date_interval(self, date: datetime) -> Tuple[datetime, datetime]:
        time_delta: timedelta = timedelta(
            days=self.config.day, hours=self.config.hour, minutes=self.config.minute
        )
        from_date = date - time_delta
        to_date = date + time_delta
        return from_date, to_date

    def search(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:

        from_date, to_date = self.get_date_interval(search_request.date)

        try:
            # Request file list from API
            file_list = self.client.historic.get_file_list(
                sport=self.cfg.betfair_football.sport,
                plan=self.cfg.betfair_football.plan,
                from_day=str(from_date.day),
                from_month=str(from_date.month),
                from_year=str(from_date.year),
                to_day=str(to_date.day),
                to_month=str(to_date.month),
                to_year=str(to_date.year),
                market_types_collection=list(self.cfg.betfair_football.markets),
                countries_collection=[search_request.country],
                file_type_collection=list(self.cfg.betfair_football.file_type),
                event_name=self.get_football_event_name(search_request),
            )

            # Log success and file count
            logger.info(
                f"Successfully retrieved {len(file_list)} files for period "
                f"{from_date.date()} to {to_date.date()}"
            )

            return file_list

        except betfairlightweight.exceptions.BetfairError as e:
            error_msg = f"Failed to retrieve file list: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error retrieving file list: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)


########################################
# Main class
########################################
class BetfairSearchEngine:
    """
    Searches for matches on Betfair with multiple strategies
    """

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
    ):

        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]

        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )

        self.search_strategies = self.set_search_strategies()

    def set_search_strategies(self) -> BaseSearchStrategy:
        search_strategies_mapping: Dict[str, Type[BaseSearchStrategy]] = {
            "ExactDateTeamSearch": ExactDateTeamSearch,
            # TODO: Add more strategies
        }
        # return [
        #     search_strategies_mapping[strategy_name](cfg=self.cfg, client=self.client)
        #     for strategy_name in self.cfg.search.active_strategies
        #     if strategy_name in search_strategies_mapping
        # ]
        return search_strategies_mapping[self.cfg.search.active_strategies[0]](
            cfg=self.cfg, client=self.client
        )

    def search_match(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        """
        Search for a match using multiple strategies

        Returns:
            SearchResult with:
                - match_id: Optional[str]
                - confidence: float (0-1)
                - strategy_used: str
                - search_attempts: List[SearchAttempt]
        """
        return self.search_strategies.search(search_request)

    # def bulk_search(self, params_list: List[BetfairSearchParams]) -> List[SearchResult]:
    #     """
    #     Efficiently search for multiple matches
    #     Uses caching and rate limiting
    #     """
    #     pass
