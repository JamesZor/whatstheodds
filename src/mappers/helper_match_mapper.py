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
from src.extractors import MatchExtractor
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

        self.extractor: MatchExtractor = MatchExtractor(config=self.cfg)

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

    def extract_event_details(
        self, download_result: Dict[str, Union[Path, str]]
    ) -> Dict[str, Dict[str, str]]:
        """
        Extract event details from downloaded match files using match_id only

        Args:
            download_result: Dict mapping match_id to either downloaded file Path or error message

        Returns:
            Dict mapping match_id to extracted match details

        Example return:
            {
                "match_id_1": {
                    "home": "Liverpool",
                    "away": "Newcastle"
                }
            }
        """
        match_details: Dict[str, Dict[str, str]] = {}

        # Filter out failed downloads (only process successful ones)
        valid_downloads = {
            match_id: path
            for match_id, path in download_result.items()
            if isinstance(path, Path)
        }

        total_valid = len(valid_downloads)
        failed_downloads = len(download_result) - total_valid

        if failed_downloads > 0:
            logger.warning(
                f"Skipping {failed_downloads} failed downloads for extraction"
            )

        if total_valid == 0:
            logger.warning("No valid downloads found for extraction")
            return match_details

        logger.info(f"Extracting details for {total_valid} matches")

        # Track statistics
        successful_extractions = 0
        failed_extractions = 0

        with tqdm(
            valid_downloads.keys(),
            desc="Extracting match details",
            unit="match",
            total=total_valid,
            ncols=100,
        ) as pbar:

            for match_id in pbar:
                try:
                    # Update progress description
                    pbar.set_description(f"Extracting {match_id[:8]}...")

                    # Extract using only match_id (Option 2)
                    extract_result = self.extractor.extract_match(match_id=match_id)

                    # Validate extraction result
                    if not extract_result or "match_info" not in extract_result:
                        raise ValueError(
                            "Invalid extraction result - missing match_info"
                        )

                    event = extract_result["match_info"]

                    # Extract basic team information
                    match_details[match_id] = {
                        "home": event.get("home_team", "Unknown"),
                        "away": event.get("away_team", "Unknown"),
                    }

                    successful_extractions += 1

                    pbar.set_postfix(
                        {
                            "success": successful_extractions,
                            "failed": failed_extractions,
                        }
                    )

                except Exception as e:
                    error_msg = f"Extraction failed: {str(e)}"
                    logger.error(f"Match {match_id}: {error_msg}")

                    match_details[match_id] = {
                        "home": "Extraction Failed",
                        "away": "Extraction Failed",
                    }

                    failed_extractions += 1

                    pbar.set_postfix(
                        {
                            "success": successful_extractions,
                            "failed": failed_extractions,
                        }
                    )

        logger.info(
            f"Extraction complete: {successful_extractions} successful, {failed_extractions} failed"
        )

        return match_details

    def extract_team_names(self, match_details: Dict[str, Dict[str, str]]) -> set[str]:
        """
        Extract all unique team names from match details, excluding None values

        Args:
            match_details: Dict mapping match_id to home/away team info

        Returns:
            Set of unique team names
        """
        team_names = set()

        for match_id, teams in match_details.items():
            home_team = teams.get("home")
            away_team = teams.get("away")

            # Add home team if it's not None
            if home_team is not None:
                team_names.add(home_team)

            # Add away team if it's not None
            if away_team is not None:
                team_names.add(away_team)

        logger.info(f"Extracted {len(team_names)} unique team names")
        return team_names

    def process_matches_for_teams(
        self, date: datetime, date_delta: timedelta
    ) -> set[str]:
        """
        Complete processing pipeline to extract team names from matches within a date range

        Args:
            date: Target date for match search
            date_delta: Time delta to create date range (date ± date_delta)

        Returns:
            Set of unique team names found in the matches

        Raises:
            ConnectionError: If Betfair API connection fails
            RuntimeError: If any step in the pipeline fails
        """
        logger.info(
            f"Starting match processing pipeline for date {date.date()} ± {date_delta}"
        )

        try:
            # Step 1: Search for matches
            logger.info("Step 1/4: Searching for matches...")
            search_results = self.search_all_match_via_date(date, date_delta)

            if not search_results.events:
                logger.warning("No matches found in search results")
                return set()

            # Step 2: Download match files
            logger.info("Step 2/4: Downloading match files...")
            download_results = self.download_matches(search_results)

            # Step 3: Extract match details
            logger.info("Step 3/4: Extracting match details...")
            extract_results = self.extract_event_details(download_results)

            # Step 4: Extract team names
            logger.info("Step 4/4: Extracting team names...")
            team_names = self.extract_team_names(extract_results)

            logger.info(
                f"Pipeline completed successfully. Found {len(team_names)} unique teams"
            )
            return team_names

        except (ConnectionError, RuntimeError) as e:
            # Re-raise known exceptions
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            error_msg = f"Unexpected error in processing pipeline: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def extract_team_names_from_tmp(self) -> set[str]:
        """
        Extract team names from all match files already downloaded in tmp directory

        Returns:
            Set of unique team names from cached files
        """
        logger.info("Scanning tmp directory for downloaded match files...")

        team_names = set()
        processed_matches = 0
        failed_extractions = 0

        try:

            match_folders = self._scan_tmp_for_matches()

            total_folders = len(match_folders)
            logger.info(f"Found {total_folders} match folders in tmp directory")

            if total_folders == 0:
                logger.warning("No match folders found in tmp directory")
                return team_names

            with tqdm(
                match_folders, desc="Processing cached matches", unit="match"
            ) as pbar:
                for match_folder in pbar:
                    try:
                        # Extract match_id from folder name/path
                        match_id = self._extract_match_id_from_folder(match_folder)

                        if not match_id:
                            logger.warning(
                                f"Could not extract match_id from folder: {match_folder}"
                            )
                            continue

                        pbar.set_description(f"Processing {match_id[:8]}...")

                        # Use your existing extractor
                        extract_result = self.extractor.extract_match(match_id=match_id)

                        if extract_result and "match_info" in extract_result:
                            event = extract_result["match_info"]

                            # Add team names if they exist and aren't None
                            home_team = event.get("home_team")
                            away_team = event.get("away_team")

                            if home_team:
                                team_names.add(home_team)
                            if away_team:
                                team_names.add(away_team)

                            processed_matches += 1
                        else:
                            failed_extractions += 1

                        pbar.set_postfix(
                            {
                                "teams": len(team_names),
                                "processed": processed_matches,
                                "failed": failed_extractions,
                            }
                        )

                    except Exception as e:
                        logger.warning(
                            f"Failed to process match folder {match_folder}: {str(e)}"
                        )
                        failed_extractions += 1

            logger.info(
                f"Extraction from cache complete: {processed_matches} matches processed, "
                f"{failed_extractions} failed, {len(team_names)} unique teams found"
            )

            return team_names

        except Exception as e:
            error_msg = f"Failed to extract team names from tmp directory: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _scan_tmp_for_matches(self) -> List[Path]:
        """Scan tmp directory for match folders"""
        from pathlib import Path

        # Get the tmp root directory from your storage manager
        tmp_root = Path(self.tmp_storage.dir_temp)  # You might need to adjust this

        match_folders = []

        # Look for directories that contain betfair files
        for item in tmp_root.rglob("*"):
            if item.is_dir():
                # Check if this directory contains betfair files (.bz2 files typically)
                if any(
                    file.suffix == ".bz2" for file in item.iterdir() if file.is_file()
                ):
                    match_folders.append(item)

        return match_folders

    def _extract_match_id_from_folder(self, folder_path: Path) -> Optional[str]:
        """Extract match_id from folder path or folder name"""

        # Option 1: If match_id is the folder name
        folder_name = folder_path.name
        if folder_name.isdigit():  # Match IDs appear to be numeric
            return folder_name

        # Option 2: If match_id is part of the path
        # Based on your file structure: /path/to/tmp/match_id/
        parts = folder_path.parts
        for part in reversed(parts):
            if part.isdigit() and len(part) >= 8:  # Match IDs seem to be 8+ digits
                return part

        # Option 3: Look for files in the folder and extract match_id from file names
        try:
            for file in folder_path.iterdir():
                if file.is_file() and ".bz2" in file.name:
                    # Extract from file path like: /path/31679991/1.202358758.bz2
                    file_parts = str(file).split("/")
                    for part in file_parts:
                        if part.isdigit() and len(part) >= 8:
                            return part
        except Exception:
            pass

        logger.warning(f"Could not extract match_id from folder: {folder_path}")
        return None
