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

    def _handle_multifiles(
        self,
        market_type: str,
        file_list: list[str],
        api_params: dict[str, Any],
        search_request: BetfairSearchRequest,
    ) -> BetfairSearchSingleMarketResult:

        event_ids = set([f.split("/")[-2] for f in file_list])

        if len(event_ids) == 1:
            file_list.sort()
            combined_paths = ",".join(file_list)

            raw_filename = file_list[0].split("/")[-1].replace(".bz2", "")
            first_market_id = raw_filename.split(".")[-1]

            logger.info(
                f"[{search_request.sofa_match_id}] Split file detected. Stitching {len(file_list)} files."
            )

            return BetfairSearchSingleMarketResult(
                strategy_used=self.name,
                market_type=market_type,
                file=combined_paths,
                match_id=str(search_request.sofa_match_id),
                market_id=first_market_id,
            )
        else:
            # ---> YOU WERE MISSING THIS BLOCK! <---
            logger.warning(
                f"\n[MATCH {search_request.sofa_match_id}] MULTIPLE EVENTS for {market_type}.\n"
                f"Sent Event Name: '{api_params['event_name']}'\n"
                f"Files Returned: {file_list}\n"
            )
            return BetfairSearchSingleMarketResult(
                strategy_used=self.name,
                market_type=market_type,
                file=None,
                match_id=None,
                market_id=None,
                error=f"Multiple distinct events found: {event_ids}",
            )

    def _handle_empty_filelist(
        self,
        market_type: str,
        file_list: list[str],
        api_params: dict[str, Any],
        search_request: BetfairSearchRequest,
    ) -> BetfairSearchSingleMarketResult:
        """
        Helper to process the failed results, no file list
        """

        # DEBUG: Print the event name and date range we sent that resulted in a total miss
        logger.warning(
            f"\n[MATCH {search_request.sofa_match_id}] NO FILES for {market_type}.\n"
            f"Sent Event Name: '{api_params['event_name']}'\n"
            f"Date Range: {api_params['from_year']}-{api_params['from_month']}-{api_params['from_day']} to {api_params['to_day']}\n"
        )
        return BetfairSearchSingleMarketResult(
            strategy_used=self.name,
            market_type=market_type,
            file=None,
            match_id=None,
            market_id=None,
            error=f"[MATCH {search_request.sofa_match_id}] NO FILES for {market_type} : api_params={api_params}",
        )


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
        api_params = {
            "sport": self.cfg.betfair_football.sport,
            "plan": self.cfg.betfair_football.plan,
            "from_day": str(from_date.day),
            "from_month": str(from_date.month),
            "from_year": str(from_date.year),
            "to_day": str(to_date.day),
            "to_month": str(to_date.month),
            "to_year": str(to_date.year),
            "market_types_collection": [market_type],
            "countries_collection": [search_request.country],
            "file_type_collection": list(self.cfg.betfair_football.file_type),
            "event_name": self.get_football_event_name(search_request),
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with RateLimitedContext():
                    file_list = self.client.historic.get_file_list(**api_params)

                # --- 1. HANDLE MULTIPLE FILES ---
                if len(file_list) > 1:
                    return self._handle_multifiles(
                        market_type, file_list, api_params, search_request
                    )

                # --- 2. HANDLE EMPTY FILES ---
                if not file_list:
                    return self._handle_empty_filelist(
                        market_type, file_list, api_params, search_request
                    )

                # --- 3. HANDLE SINGLE SUCCESS ---
                return BetfairSearchSingleMarketResult.from_path_string(
                    file=file_list[0],
                    strategy_used_name=self.name,
                    market_type=market_type,
                )

            except betfairlightweight.exceptions.BetfairError as e:
                # --- 4. GRACEFULLY HANDLE 400 ERRORS (Locked Data) ---
                if "400" in str(e):
                    logger.warning(
                        f"[{search_request.sofa_match_id}] API 400 Error for {market_type}. Data likely locked or unavailable."
                    )
                    return BetfairSearchSingleMarketResult(
                        strategy_used=self.name,
                        market_type=market_type,
                        file=None,
                        match_id=None,
                        market_id=None,
                        error=f"API 400 Error (Data locked). Params: {api_params}",
                    )

                # --- 5. RETRY ON SERVER ERRORS (502, 503, 504) ---
                if "50" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[{search_request.sofa_match_id}] Betfair 50x Server Error for {market_type}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})"
                        )
                        time.sleep(2)
                        continue  # Try the loop again!

                # If we run out of retries, DO NOT RAISE. Return a failed market result to save the rest of the match!
                error_msg = f"API Status: {str(e)} | Sent Params: {api_params}"
                logger.error(error_msg)
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error=error_msg,
                )

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue

                error_msg = f"Unexpected error: {str(e)} | Sent Params: {api_params}"
                logger.error(error_msg)
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error=error_msg,
                )

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

        api_params = {
            "sport": self.cfg.betfair_football.sport,
            "plan": self.cfg.betfair_football.plan,
            "from_day": str(from_date.day),
            "from_month": str(from_date.month),
            "from_year": str(from_date.year),
            "to_day": str(to_date.day),
            "to_month": str(to_date.month),
            "to_year": str(to_date.year),
            "market_types_collection": [market_type],
            "countries_collection": [search_request.country],
            "file_type_collection": list(self.cfg.betfair_football.file_type),
            "event_name": self.get_football_event_name(search_request),
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with RateLimitedContext():
                    file_list = self.client.historic.get_file_list(**api_params)

                # --- 1. HANDLE MULTIPLE FILES ---
                if len(file_list) > 1:
                    return self._handle_multifiles(
                        market_type, file_list, api_params, search_request
                    )

                # --- 2. HANDLE EMPTY FILES ---
                if not file_list:
                    return self._handle_empty_filelist(
                        market_type, file_list, api_params, search_request
                    )

                # --- 3. HANDLE SINGLE SUCCESS ---
                return BetfairSearchSingleMarketResult.from_path_string(
                    file=file_list[0],
                    strategy_used_name=self.name,
                    market_type=market_type,
                )

            except betfairlightweight.exceptions.BetfairError as e:
                # --- 4. GRACEFULLY HANDLE 400 ERRORS (Locked Data) ---
                if "400" in str(e):
                    logger.warning(
                        f"[{search_request.sofa_match_id}] API 400 Error for {market_type}. Data likely locked or unavailable."
                    )
                    return BetfairSearchSingleMarketResult(
                        strategy_used=self.name,
                        market_type=market_type,
                        file=None,
                        match_id=None,
                        market_id=None,
                        error=f"API 400 Error (Data locked). Params: {api_params}",
                    )

                # --- 5. RETRY ON SERVER ERRORS (502, 503, 504) ---
                if "50" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[{search_request.sofa_match_id}] Betfair 50x Server Error for {market_type}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})"
                        )
                        time.sleep(2)
                        continue  # Try the loop again!

                # If we run out of retries, DO NOT RAISE. Return a failed market result to save the rest of the match!
                error_msg = f"API Status: {str(e)} | Sent Params: {api_params}"
                logger.error(error_msg)
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error=error_msg,
                )

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue

                error_msg = f"Unexpected error: {str(e)} | Sent Params: {api_params}"
                logger.error(error_msg)
                return BetfairSearchSingleMarketResult(
                    strategy_used=self.name,
                    market_type=market_type,
                    file=None,
                    match_id=None,
                    market_id=None,
                    error=error_msg,
                )

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
