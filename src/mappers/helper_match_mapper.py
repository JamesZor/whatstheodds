import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

from src.betfair.api_client import setup_betfair_api_client
from src.betfair.dataclasses import (
    BetfairSearchResultMultiDate,
    BetfairSearchSingleMarketResult,
)
from src.betfair.downloader import BetfairDownloader
from src.storage.temp_manager import TempStorage
from src.utils import load_config

logger = logging.getLogger(__name__)


# HACK:
class HelperMapper:

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
        tmp_manager: Optional[TempStorage] = None,
    ):

        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]

        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )

        self.tmp_storage: TempStorage = (
            TempStorage(cfg=self.cfg) if tmp_manager is None else tmp_manager
        )

        self.downloader: BetfairDownloader = BetfairDownloader(
            api_client=self.client, tmp_manager=self.tmp_storage, cfg=self.cfg
        )

    def get_date_interval(
        self, date: datetime, date_delta: timedelta
    ) -> Tuple[datetime, datetime]:
        from_date = date - date_delta
        to_date = date + date_delta
        return from_date, to_date

    def search_all_match_via_date(
        self, date: datetime, date_delta: timedelta
    ) -> BetfairSearchResultMultiDate:
        from_date, to_date = self.get_date_interval(date=date, date_delta=date_delta)
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
                market_types_collection=["MATCH_ODDS"],  # HACK:
                countries_collection=["GB"],  # HACK:
                file_type_collection=list(self.cfg.betfair_football.file_type),
            )

            # Log success and file count
            logger.info(
                f"Successfully retrieved {len(file_list)} files for period "
                f"{from_date.date()} to {to_date.date()}"
            )

            # Convert file paths to BetfairSearchSingleMarketResult objects
            search_results = []
            for file_path in file_list:
                try:
                    result = BetfairSearchSingleMarketResult.from_path_string(
                        file=file_path,
                        strategy_used_name="historic_search",  # or whatever strategy name you want
                        market_type="MATCH_ODDS",  # or extract from your config
                    )
                    search_results.append(result)
                except Exception as e:
                    logger.warning(f"Failed to parse file path '{file_path}': {str(e)}")
                    # Optionally create an error result
                    error_result = BetfairSearchSingleMarketResult(
                        strategy_used="helper_search",
                        market_type="MATCH_ODDS",
                        file=file_path,
                        error=str(e),
                    )
                    search_results.append(error_result)

            return BetfairSearchResultMultiDate.from_results_list(
                search_results, to_date, from_date
            )

        except betfairlightweight.exceptions.BetfairError as e:
            error_msg = f"Failed to retrieve file list: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error retrieving file list: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def download_matches(
        self, search_results: BetfairSearchResultMultiDate
    ) -> Dict[str, Union[Path, str]]:
        """
        Download all matches from search results with progress tracking

        Args:
            search_results: Search results containing match information

        Returns:
            Dict mapping match_id to either downloaded file Path or error message
        """
        downloaded: Dict[str, Union[Path, str]] = {}

        # Get total count for progress bar
        total_matches = len(search_results.events)

        if total_matches == 0:
            logger.warning("No matches found in search results")
            return downloaded

        logger.info(f"Starting download of {total_matches} matches")

        # Create progress bar
        with tqdm(
            total=total_matches, desc="Downloading matches", unit="file", ncols=100
        ) as pbar:

            for match_id, search_result in search_results.events.items():
                try:
                    # Update progress bar description with current match
                    pbar.set_description(f"Downloading match {match_id}")

                    # Skip if search result has errors
                    if search_result.error:
                        error_msg = f"Skipping download due to search error: {search_result.error}"
                        logger.warning(error_msg)
                        downloaded[match_id] = error_msg
                        pbar.update(1)
                        continue

                    # Download the match file
                    download_result = self.downloader._download_single_market(
                        search_result
                    )  # Fixed: removed underscore
                    downloaded[match_id] = download_result

                    logger.debug(f"Successfully downloaded match {match_id}")

                except Exception as e:
                    error_msg = f"Failed to download match {match_id}: {str(e)}"
                    logger.error(error_msg)
                    downloaded[match_id] = error_msg

                finally:
                    # Always update progress bar
                    pbar.update(1)

        # Log summary
        successful_downloads = sum(
            1 for result in downloaded.values() if isinstance(result, Path)
        )
        failed_downloads = total_matches - successful_downloads

        logger.info(
            f"Download complete: {successful_downloads} successful, {failed_downloads} failed"
        )

        return downloaded
