import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

from .dataclasses import (
    BetfairSearchRequest,
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)
from .rate_limiter import RateLimitedContext, betfair_rate_limiter

logger = logging.getLogger(__name__)


########################################
# TODO: search strategies
########################################
class BaseSearchStrategy(ABC):

    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        self.cfg: DictConfig = cfg
        self.client: betfairlightweight.APIClient = client

        # HACK:
        self.config = self.cfg.search.exact_date_team_search
        self.config_1 = self.cfg.search.extended_strategy_conf

    def get_football_event_name(self, search_request: BetfairSearchRequest) -> str:
        return f"{search_request.home} v {search_request.away}"

    @abstractmethod
    def search(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        pass

    @abstractmethod
    def _search_per_market_type(
        self, search_request: BetfairSearchRequest, market_type: str
    ) -> BetfairSearchSingleMarketResult:
        pass

    @abstractmethod
    def search_over_config_markets(
        self,
        search_request: BetfairSearchRequest,
    ) -> List[BetfairSearchSingleMarketResult]:
        pass


class ExactDateTeamSearch(BaseSearchStrategy):
    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        super().__init__(cfg=cfg, client=client)
        self.name: str = "exact_date_team_search"

    def get_date_interval(self, date: datetime) -> Tuple[datetime, datetime]:
        time_delta: timedelta = timedelta(
            days=self.config.day, hours=self.config.hour, minutes=self.config.minute
        )
        from_date = date - time_delta
        to_date = date + time_delta
        return from_date, to_date

    def _search_per_market_type(
        self, search_request: BetfairSearchRequest, market_type: str
    ) -> BetfairSearchSingleMarketResult:  # [ignore : override ]

        from_date, to_date = self.get_date_interval(search_request.date)

        try:
            # Request file list from API
            with RateLimitedContext():
                file_list = self.client.historic.get_file_list(
                    sport=self.cfg.betfair_football.sport,
                    plan=self.cfg.betfair_football.plan,
                    from_day=str(from_date.day),
                    from_month=str(from_date.month),
                    from_year=str(from_date.year),
                    to_day=str(to_date.day),
                    to_month=str(to_date.month),
                    to_year=str(to_date.year),
                    market_types_collection=[market_type],
                    countries_collection=[search_request.country],
                    file_type_collection=list(self.cfg.betfair_football.file_type),
                    event_name=self.get_football_event_name(search_request),
                )

            # Log success and file count
            logger.info(
                f"Successfully retrieved {len(file_list)} files for period, for market_type: {market_type} "
            )

            if len(file_list) > 1:
                logger.warning(
                    f"More than one file found for {search_request.sofa_match_id}."
                )
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error="More than one file.",
                )

            if not file_list:
                logger.warning(f"No file found for {search_request.sofa_match_id}.")
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error="No files found.",
                )

            return BetfairSearchSingleMarketResult.from_path_string(
                file=file_list[0], strategy_used_name=self.name, market_type=market_type
            )

        except betfairlightweight.exceptions.BetfairError as e:
            error_msg = f"Failed to retrieve file list: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error retrieving file list: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)  # type: ignore[override]  # type: ignore[override]

    def search_over_config_markets(
        self, search_request: BetfairSearchRequest
    ) -> List[BetfairSearchSingleMarketResult]:
        results: List[BetfairSearchSingleMarketResult] = []
        markets = list(self.cfg.betfair_football.markets)

        progress_bar = tqdm(markets, desc="Searching markets")
        for market_type in progress_bar:
            progress_bar.set_postfix({"current": market_type})
            logger.info(f"searching: {market_type}..")
            time_search_start = time.time()

            results.append(
                self._search_per_market_type(
                    search_request=search_request, market_type=market_type
                )
            )

            time_search_end = time.time()
            duration = time_search_end - time_search_start
            logger.info(f"completed: {market_type} in {duration:.2f} seconds")
            progress_bar.set_postfix({"last_duration": f"{duration:.2f}s"})

        return results

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


class ExtendDateTeamSearch(BaseSearchStrategy):
    def __init__(self, cfg: DictConfig, client: betfairlightweight.APIClient):
        super().__init__(cfg=cfg, client=client)
        self.name: str = "extend_date_team_search"
        self.config = self.cfg.search.extended_strategy_conf

    def get_date_interval(self, date: datetime) -> Tuple[datetime, datetime]:
        time_delta: timedelta = timedelta(
            days=self.config.day, hours=self.config.hour, minutes=self.config.minute
        )
        from_date = date - time_delta
        to_date = date + time_delta
        return from_date, to_date

    def _search_per_market_type(
        self, search_request: BetfairSearchRequest, market_type: str
    ) -> BetfairSearchSingleMarketResult:  # [ignore : override ]

        from_date, to_date = self.get_date_interval(search_request.date)

        try:
            with RateLimitedContext():
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
                    market_types_collection=[market_type],
                    countries_collection=[search_request.country],
                    file_type_collection=list(self.cfg.betfair_football.file_type),
                    event_name=self.get_football_event_name(search_request),
                )

            # Log success and file count
            logger.info(
                f"Successfully retrieved {len(file_list)} files for period, for market_type: {market_type} "
            )

            if len(file_list) > 1:
                logger.warning(
                    f"More than one file found for {search_request.sofa_match_id}."
                )
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error="More than one file.",
                )

            if not file_list:
                logger.warning(f"No file found for {search_request.sofa_match_id}.")
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error="No files found.",
                )

            return BetfairSearchSingleMarketResult.from_path_string(
                file=file_list[0], strategy_used_name=self.name, market_type=market_type
            )

        except betfairlightweight.exceptions.BetfairError as e:
            error_msg = f"Failed to retrieve file list: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error retrieving file list: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)  # type: ignore[override]  # type: ignore[override]

    def search_over_config_markets(
        self, search_request: BetfairSearchRequest
    ) -> List[BetfairSearchSingleMarketResult]:

        results: List[BetfairSearchSingleMarketResult] = []

        for market_type in list(self.cfg.betfair_football.markets):
            logger.info(f"searching: {market_type}..")
            results.append(
                self._search_per_market_type(
                    search_request=search_request, market_type=market_type
                )
            )
            logger.info(f"found: {market_type}.")

        return results

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
