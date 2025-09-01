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

# from whatstheodds.betfair.downloader import BetfairDownloader
from whatstheodds.betfair.downloader_enhanced import BetfairDownloader
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.extractors import MatchExtractor
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.pipeline.pipeline_coordinator import (
    ProcessingResult,
    ProcessingStatus,
)
from whatstheodds.processor.state_manager import ProcessingStateManager
from whatstheodds.storage.temp_manager import TempStorage
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


class PipelineCoordinator:
    """
    Coordinates the entire pipeline from match row to extracted odds data
    """

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
        tmp_manager: Optional[TempStorage] = None,
        match_mapper: Optional[MatchMapper] = None,
        search_engine: Optional[BetfairSearchEngine] = None,
        downloader: Optional[BetfairDownloader] = None,
        extractor: Optional[MatchExtractor] = None,
    ):
        """
        Initialize the pipeline coordinator

        Args:
            api_client: Betfair API client (optional, will create if None)
            cfg: Configuration object (optional, will load if None)
            tmp_manager: Temporary storage manager (optional, will create if None)
            match_mapper: Match mapper instance (optional, will create if None)
            search_engine: Search engine instance (optional, will create if None)
            downloader: Downloader instance (optional, will create if None)
            extractor: Match extractor instance (optional, will create if None)
        """
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]

        # Initialize core components
        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )

        self.tmp_storage: TempStorage = (
            TempStorage(cfg=self.cfg) if tmp_manager is None else tmp_manager
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

        self.downloader: BetfairDownloader = (
            BetfairDownloader(
                api_client=self.client, tmp_manager=self.tmp_storage, cfg=self.cfg
            )
            if downloader is None
            else downloader
        )

        self.extractor: MatchExtractor = (
            MatchExtractor(cfg=self.cfg) if extractor is None else extractor
        )

        # Processing statistics
        self.stats = {
            "processed": 0,
            "successful": 0,
            "mapping_failures": 0,
            "search_failures": 0,
            "download_failures": 0,
            "extraction_failures": 0,
            "unexpected_errors": 0,
        }

        logger.info("PipelineCoordinator initialized successfully")

    # enhancements chunk 4

    # number 1
    def process_from_row_with_state(
        self,
        row: pd.Series,
        state_manager: ProcessingStateManager,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team_slug",
        away_column: str = "away_team_slug",
        tournament_column: str = "tournament_id",
        cleanup_on_success: bool = False,
        cleanup_on_failure: bool = False,
        archive_on_success: bool = True,
        skip_archived: bool = False,
    ) -> ProcessingResult:
        """
        Process a single match with state tracking

        Args:
            row: Pandas Series containing match information
            state_manager: ProcessingStateManager instance
            date_column: Name of the date column
            match_id_column: Name of match ID column
            home_column: Name of home team column
            away_column: Name of away team column
            tournament_column: Name of tournament ID column
            cleanup_on_success: Whether to cleanup temp files after success
            cleanup_on_failure: Whether to cleanup temp files after failure
            archive_on_success: Whether to archive successful matches
            skip_archived: Whether to skip matches already archived

        Returns:
            ProcessingResult with status and extracted data
        """
        start_time = datetime.now()
        sofa_match_id = None

        try:
            # Extract match ID
            sofa_match_id = int(row[match_id_column])
            logger.info(f"Processing match {sofa_match_id} with state tracking")

            # Check existing state
            existing_state = state_manager.get_match_state(sofa_match_id)

            # Skip if already archived and skip_archived is True
            if skip_archived and existing_state and existing_state.archive_path:
                logger.info(f"Match {sofa_match_id} already archived, skipping")
                return ProcessingResult(
                    sofa_match_id=sofa_match_id,
                    status=ProcessingStatus.SUCCESS,
                    betfair_match_id=existing_state.betfair_id,
                    error_message="Already archived",
                )

            # Step 1: Map match (always needed for team names)
            search_request = self._map_match_from_row(
                row,
                date_column,
                match_id_column,
                home_column,
                away_column,
                tournament_column,
            )
            if search_request is None:
                state_manager.update_match_search(
                    sofa_id=sofa_match_id, search_error="Failed to map team names"
                )
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.MAPPING_FAILED,
                    "Failed to map team names to Betfair format",
                )

            # Step 2: Search (use cache if available)
            search_result = None
            if (
                existing_state
                and existing_state.betfair_id
                and existing_state.search_cached
            ):
                # Use cached search result
                logger.info(f"Using cached search result for match {sofa_match_id}")
                search_result = BetfairSearchResult(
                    match_id=existing_state.betfair_id,
                    valid_markets=existing_state.search_result or {},
                )
            else:
                # Perform new search
                logger.info(f"Searching for match {sofa_match_id} on Betfair")
                search_result = self._search_match(search_request)

                if search_result and search_result.match_id:
                    # Cache the search result
                    state_manager.update_match_search(
                        sofa_id=sofa_match_id,
                        betfair_id=search_result.match_id,
                        search_result=search_result.valid_markets,
                    )
                else:
                    state_manager.update_match_search(
                        sofa_id=sofa_match_id, search_error="Match not found on Betfair"
                    )
                    return self._create_failed_result(
                        sofa_match_id,
                        ProcessingStatus.SEARCH_FAILED,
                        "Match not found on Betfair",
                        search_request=search_request,
                    )

            # Step 3: Download with tracking
            logger.info(f"Downloading markets for match {sofa_match_id}")
            downloaded_files, market_status = (
                self.downloader.download_odds_with_tracking(search_result)
            )

            # Update state with download status
            state_manager.update_match_downloads(sofa_match_id, market_status)

            if not downloaded_files:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.DOWNLOAD_FAILED,
                    "Failed to download any market data",
                    search_request=search_request,
                    search_result=search_result,
                    betfair_match_id=search_result.match_id,
                )

            # Step 4: Extract odds
            logger.info(f"Extracting odds for match {sofa_match_id}")
            extracted_data = self._extract_odds_data(search_result.match_id)

            if extracted_data is None:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.EXTRACTION_FAILED,
                    "Failed to extract odds data",
                    search_request=search_request,
                    search_result=search_result,
                    downloaded_files=downloaded_files,
                    betfair_match_id=search_result.match_id,
                )

            # Step 5: Archive if successful and no failures
            updated_state = state_manager.get_match_state(sofa_match_id)
            if archive_on_success and updated_state.is_fully_successful():
                archive_path = self._archive_match(
                    match_id=search_result.match_id,
                    run_name=state_manager.run_name,
                    cleanup=cleanup_on_success,
                )
                if archive_path:
                    state_manager.update_archive_path(sofa_match_id, str(archive_path))
                    logger.info(f"Archived match {sofa_match_id} to {archive_path}")

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()

            # Create successful result
            result = ProcessingResult(
                sofa_match_id=sofa_match_id,
                status=ProcessingStatus.SUCCESS,
                search_request=search_request,
                search_result=search_result,
                downloaded_files=downloaded_files,
                extracted_data=extracted_data,
                processing_time_seconds=processing_time,
                betfair_match_id=search_result.match_id,
                home_team_mapped=search_request.home,
                away_team_mapped=search_request.away,
                markets_found=list(search_result.valid_markets.keys()),
                markets_downloaded=list(downloaded_files.keys()),
            )

            self.stats["successful"] += 1

            logger.info(
                f"Successfully processed match {sofa_match_id} "
                f"(Betfair: {search_result.match_id}) in {processing_time:.2f}s"
            )

            return result

        except Exception as e:
            error_msg = f"Unexpected error processing match {sofa_match_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if sofa_match_id:
                state_manager.update_match_search(
                    sofa_id=sofa_match_id, search_error=f"Processing error: {str(e)}"
                )

            self.stats["unexpected_errors"] += 1

            return self._create_failed_result(
                sofa_match_id or -1, ProcessingStatus.UNEXPECTED_ERROR, error_msg
            )

        finally:
            self.stats["processed"] += 1

    def retry_failed_matches(
        self,
        state_manager: ProcessingStateManager,
        max_retries: Optional[int] = None,
        cleanup_on_success: bool = False,
        archive_on_success: bool = True,
    ) -> List[ProcessingResult]:
        """
        Retry only failed markets using cached search results

        Args:
            state_manager: ProcessingStateManager with match states
            max_retries: Maximum number of matches to retry (None = all)
            cleanup_on_success: Whether to cleanup temp after success
            archive_on_success: Whether to archive after success

        Returns:
            List of ProcessingResults for retried matches
        """
        # Get retry candidates
        retry_candidates = state_manager.get_retry_candidates()

        if not retry_candidates:
            logger.info("No matches need retry")
            return []

        logger.info(f"Found {len(retry_candidates)} matches needing retry")

        # Limit retries if specified
        if max_retries:
            retry_candidates = retry_candidates[:max_retries]

        results = []

        with tqdm(
            total=len(retry_candidates), desc="Retrying failed matches", unit="match"
        ) as pbar:
            for sofa_id, match_state in retry_candidates:
                pbar.set_description(f"Retrying match {sofa_id}")

                try:
                    # Get failed markets
                    failed_markets = match_state.get_failed_markets()

                    if not failed_markets:
                        logger.warning(f"No failed markets for match {sofa_id}")
                        continue

                    logger.info(
                        f"Retrying {len(failed_markets)} failed markets for match {sofa_id} "
                        f"(Betfair: {match_state.betfair_id})"
                    )

                    # Reconstruct search result from cached data
                    if not match_state.search_result:
                        logger.error(f"No cached search result for match {sofa_id}")
                        continue

                    # Create search result with only failed markets
                    search_result = BetfairSearchResult(
                        match_id=match_state.betfair_id,
                        valid_markets={
                            market: match_state.search_result.get(market, {})
                            for market in failed_markets
                            if market in match_state.search_result
                        },
                    )

                    # Retry only failed markets
                    retried_files = self.downloader.retry_failed_markets(
                        search_result, failed_markets
                    )

                    # Update state with retry results
                    retry_status = {
                        market: "success" if market in retried_files else "failed"
                        for market in failed_markets
                    }
                    state_manager.update_match_downloads(sofa_id, retry_status)

                    # Check if now fully successful
                    updated_state = state_manager.get_match_state(sofa_id)

                    if updated_state.is_fully_successful():
                        logger.info(f"Match {sofa_id} now fully successful after retry")

                        # Archive if requested
                        if archive_on_success:
                            archive_path = self._archive_match(
                                match_id=match_state.betfair_id,
                                run_name=state_manager.run_name,
                                cleanup=cleanup_on_success,
                            )
                            if archive_path:
                                state_manager.update_archive_path(
                                    sofa_id, str(archive_path)
                                )

                    # Create result
                    result = ProcessingResult(
                        sofa_match_id=sofa_id,
                        status=(
                            ProcessingStatus.SUCCESS
                            if retried_files
                            else ProcessingStatus.DOWNLOAD_FAILED
                        ),
                        betfair_match_id=match_state.betfair_id,
                        markets_downloaded=list(retried_files.keys()),
                        error_message=None if retried_files else "Retry failed",
                    )

                    results.append(result)
                    pbar.set_postfix(
                        {
                            "success": len(retried_files),
                            "failed": len(failed_markets) - len(retried_files),
                        }
                    )

                except Exception as e:
                    logger.error(f"Error retrying match {sofa_id}: {str(e)}")

                    result = ProcessingResult(
                        sofa_match_id=sofa_id,
                        status=ProcessingStatus.UNEXPECTED_ERROR,
                        error_message=f"Retry error: {str(e)}",
                    )
                    results.append(result)

                pbar.update(1)

        # Log summary
        successful_retries = sum(
            1 for r in results if r.status == ProcessingStatus.SUCCESS
        )
        logger.info(f"Retry complete: {successful_retries}/{len(results)} successful")

        return results

    def process_dataframe_with_state(
        self,
        df: pd.DataFrame,
        state_manager: ProcessingStateManager,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team",
        away_column: str = "away_team",
        tournament_column: str = "tournament_id",
        cleanup_on_success: bool = False,
        cleanup_on_failure: bool = False,
        archive_on_success: bool = True,
        save_state_frequency: int = 10,
    ) -> List[ProcessingResult]:
        """
        Process multiple matches with state tracking

        Args:
            df: DataFrame containing match information
            state_manager: ProcessingStateManager instance
            date_column: Name of the date column
            match_id_column: Name of match ID column
            home_column: Name of home team column
            away_column: Name of away team column
            tournament_column: Name of tournament ID column
            cleanup_on_success: Whether to cleanup temp after success
            cleanup_on_failure: Whether to cleanup temp after failure
            archive_on_success: Whether to archive successful matches
            save_state_frequency: Save state every N matches

        Returns:
            List of ProcessingResults
        """
        logger.info(
            f"Starting batch processing of {len(df)} matches with state tracking"
        )

        results = []

        with tqdm(total=len(df), desc="Processing matches", unit="match") as pbar:
            for idx, row in df.iterrows():
                try:
                    result = self.process_from_row_with_state(
                        row=row,
                        state_manager=state_manager,
                        date_column=date_column,
                        match_id_column=match_id_column,
                        home_column=home_column,
                        away_column=away_column,
                        tournament_column=tournament_column,
                        cleanup_on_success=cleanup_on_success,
                        cleanup_on_failure=cleanup_on_failure,
                        archive_on_success=archive_on_success,
                    )
                    results.append(result)

                    # Save state periodically
                    if (idx + 1) % save_state_frequency == 0:
                        state_manager.save_state()
                        logger.debug(f"State saved after {idx + 1} matches")

                    # Update progress bar
                    pbar.set_postfix(
                        {
                            "success": self.stats["successful"],
                            "failed": self.stats["processed"]
                            - self.stats["successful"],
                            "current": result.sofa_match_id,
                        }
                    )

                except Exception as e:
                    logger.error(f"Unexpected error processing row {idx}: {str(e)}")
                    results.append(
                        ProcessingResult(
                            sofa_match_id=-1,
                            status=ProcessingStatus.UNEXPECTED_ERROR,
                            error_message=f"Row processing error: {str(e)}",
                        )
                    )

                pbar.update(1)

        # Final state save
        state_manager.save_state()
        logger.info(f"Final state saved for run {state_manager.run_name}")

        self._log_batch_summary(results)
        return results

    def restore_and_reprocess(
        self, sofa_id: int, state_manager: ProcessingStateManager
    ) -> Optional[Dict[str, Any]]:
        """
        Restore match from archive and re-extract

        Args:
            sofa_id: Sofa match ID
            state_manager: ProcessingStateManager with match state

        Returns:
            Extracted data if successful, None otherwise
        """
        # Get match state
        match_state = state_manager.get_match_state(sofa_id)

        if not match_state:
            logger.error(f"No state found for match {sofa_id}")
            return None

        if not match_state.betfair_id:
            logger.error(f"No Betfair ID in state for match {sofa_id}")
            return None

        if not match_state.archive_path:
            logger.warning(f"Match {sofa_id} not archived")
            return None

        logger.info(
            f"Restoring match {sofa_id} (Betfair: {match_state.betfair_id}) from archive"
        )

        # Restore from archive
        if hasattr(self.tmp_storage, "restore_from_archive"):
            success = self.tmp_storage.restore_from_archive(
                match_id=match_state.betfair_id, run_name=state_manager.run_name
            )

            if not success:
                logger.error(f"Failed to restore match {sofa_id} from archive")
                return None
        else:
            logger.error("Storage does not support restore from archive")
            return None

        # Re-extract
        logger.info(f"Re-extracting match {sofa_id}")
        extracted_data = self.extractor.extract_match(match_state.betfair_id)

        if extracted_data:
            logger.info(f"Successfully re-processed match {sofa_id}")
        else:
            logger.error(f"Failed to re-extract match {sofa_id}")

        return extracted_data  # type: ignore[no-any-return]

    def generate_retry_report(
        self, state_manager: ProcessingStateManager
    ) -> Dict[str, Any]:
        """
        Generate detailed retry report from state

        Args:
            state_manager: ProcessingStateManager with match states

        Returns:
            Dict with retry analysis
        """
        stats = state_manager.get_statistics()

        # Get detailed information
        retry_candidates = state_manager.get_retry_candidates()
        matches_with_failures = state_manager.get_matches_with_failures()

        # Analyze failure patterns
        failure_by_market: Dict = {}
        for sofa_id, state in state_manager.processing_state.items():
            for market, status in state.markets.items():
                if status == "failed":
                    failure_by_market[market] = failure_by_market.get(market, 0) + 1

        # Find matches at max attempts
        max_attempts_reached = [
            sofa_id
            for sofa_id, state in state_manager.processing_state.items()
            if state.download_attempts >= state_manager.max_download_attempts
            and state.has_failures()
        ]

        # Find matches not found
        matches_not_found = [
            sofa_id
            for sofa_id, state in state_manager.processing_state.items()
            if state.search_error and "not found" in state.search_error.lower()
        ]

        report = {
            "run_name": state_manager.run_name,
            "total_matches": stats["total_matches"],
            "successful_matches": stats["fully_successful_matches"],
            "matches_with_failures": stats["matches_with_failures"],
            "matches_not_found": len(matches_not_found),
            "retry_candidates": [sofa_id for sofa_id, _ in retry_candidates],
            "max_attempts_reached": max_attempts_reached,
            "failure_by_market": failure_by_market,
            "total_download_attempts": stats["total_download_attempts"],
            "statistics": stats,
        }

        return report

    def _archive_match(
        self, match_id: str, run_name: str, cleanup: bool = True
    ) -> Optional[Path]:
        """
        Archive a match if HybridStorage is available

        Args:
            match_id: Betfair match ID
            run_name: Processing run name
            cleanup: Whether to cleanup temp files

        Returns:
            Archive path if successful, None otherwise
        """
        if not hasattr(self.tmp_storage, "archive_match"):
            logger.debug("Storage does not support archiving")
            return None

        try:
            archive_path = self.tmp_storage.archive_match(
                match_id=match_id, run_name=run_name, cleanup_temp=cleanup
            )
            return archive_path  # type: ignore[no-any-return]
        except Exception as e:
            logger.error(f"Failed to archive match {match_id}: {str(e)}")
            return None

    @property
    def has_state_tracking(self) -> bool:
        """Check if state tracking is enabled"""
        return hasattr(self, "state_manager") and self.state_manager is not None

    # old
    def process_from_row(
        self,
        row: pd.Series,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team_slug",
        away_column: str = "away_team_slug",
        tournament_column: str = "tournament_id",
        cleanup_on_success: bool = True,
        cleanup_on_failure: bool = False,
    ) -> ProcessingResult:
        """
        Process a single match from a pandas Series

        Args:
            row: Pandas Series containing match information
            date_column: Name of the date column
            match_id_column: Name of match ID column
            home_column: Name of home team column
            away_column: Name of away team column
            tournament_column: Name of tournament ID column
            cleanup_on_success: Whether to cleanup temp files after successful processing
            cleanup_on_failure: Whether to cleanup temp files after failed processing

        Returns:
            ProcessingResult with status and extracted data
        """
        start_time = datetime.now()
        sofa_match_id = None

        try:
            # Extract match ID first for logging
            sofa_match_id = int(row[match_id_column])
            logger.info(f"Starting processing for match {sofa_match_id}")

            # Step 1: Validate input row
            validation_result = self._validate_row(
                row, date_column, match_id_column, home_column, away_column
            )
            if not validation_result[0]:
                return self._create_failed_result(
                    sofa_match_id, ProcessingStatus.MAPPING_FAILED, validation_result[1]
                )

            # Step 2: Map match to Betfair search request
            search_request = self._map_match_from_row(
                row,
                date_column,
                match_id_column,
                home_column,
                away_column,
                tournament_column,
            )
            if search_request is None:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.MAPPING_FAILED,
                    "Failed to map team names to Betfair format",
                )

            logger.debug(
                f"Mapped teams: {search_request.home} vs {search_request.away}"
            )

            # Step 3: Search for match on Betfair
            search_result = self._search_match(search_request)
            if search_result is None or search_result.match_id is None:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.SEARCH_FAILED,
                    "Match not found on Betfair",
                    search_request=search_request,
                    home_team_mapped=search_request.home,
                    away_team_mapped=search_request.away,
                )

            logger.debug(f"Found Betfair match: {search_result.match_id}")

            # Step 4: Download match data
            downloaded_files = self._download_match_data(search_result)
            if not downloaded_files:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.DOWNLOAD_FAILED,
                    "Failed to download any market data",
                    search_request=search_request,
                    search_result=search_result,
                    betfair_match_id=search_result.match_id,
                    home_team_mapped=search_request.home,
                    away_team_mapped=search_request.away,
                    markets_found=list(search_result.valid_markets.keys()),
                )

            logger.debug(f"Downloaded {len(downloaded_files)} market files")

            # Step 5: Extract odds data
            extracted_data = self._extract_odds_data(search_result.match_id)
            if extracted_data is None:
                return self._create_failed_result(
                    sofa_match_id,
                    ProcessingStatus.EXTRACTION_FAILED,
                    "Failed to extract odds data from downloaded files",
                    search_request=search_request,
                    search_result=search_result,
                    downloaded_files=downloaded_files,
                    betfair_match_id=search_result.match_id,
                    home_team_mapped=search_request.home,
                    away_team_mapped=search_request.away,
                    markets_found=list(search_result.valid_markets.keys()),
                    markets_downloaded=list(downloaded_files.keys()),
                )

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()

            # Create successful result
            result = ProcessingResult(
                sofa_match_id=sofa_match_id,
                status=ProcessingStatus.SUCCESS,
                search_request=search_request,
                search_result=search_result,
                downloaded_files=downloaded_files,
                extracted_data=extracted_data,
                processing_time_seconds=processing_time,
                betfair_match_id=search_result.match_id,
                home_team_mapped=search_request.home,
                away_team_mapped=search_request.away,
                markets_found=list(search_result.valid_markets.keys()),
                markets_downloaded=list(downloaded_files.keys()),
            )

            # Update statistics
            self.stats["successful"] += 1

            # Cleanup if requested
            if cleanup_on_success and search_result.match_id:
                self._cleanup_temp_files(search_result.match_id)

            logger.info(
                f"Successfully processed match {sofa_match_id} "
                f"(Betfair ID: {search_result.match_id}) in {processing_time:.2f}s"
            )

            return result

        except Exception as e:
            error_msg = f"Unexpected error processing match {sofa_match_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.stats["unexpected_errors"] += 1

            # Cleanup on failure if requested
            if cleanup_on_failure and sofa_match_id:
                # Try to extract match_id from any search result we might have
                match_id = getattr(
                    getattr(locals().get("search_result"), "match_id", None), None, None
                )
                if match_id:
                    self._cleanup_temp_files(match_id)

            return self._create_failed_result(
                sofa_match_id or -1, ProcessingStatus.UNEXPECTED_ERROR, error_msg
            )

        finally:
            self.stats["processed"] += 1

    def _validate_row(
        self,
        row: pd.Series,
        date_column: str,
        match_id_column: str,
        home_column: str,
        away_column: str,
    ) -> tuple[bool, Optional[str]]:
        """Validate that the row has all required columns and data"""

        required_columns = [date_column, match_id_column, home_column, away_column]
        missing_columns = [col for col in required_columns if col not in row.index]

        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"

        # Check for missing values
        missing_values = [
            col for col in required_columns if pd.isna(row[col]) or row[col] == ""
        ]
        if missing_values:
            return False, f"Missing values in columns: {missing_values}"

        # Validate match_id is numeric
        try:
            int(row[match_id_column])
        except (ValueError, TypeError):
            return False, f"Match ID must be numeric: {row[match_id_column]}"

        return True, None

    def _map_match_from_row(
        self,
        row: pd.Series,
        date_column: str,
        match_id_column: str,
        home_column: str,
        away_column: str,
        tournament_column: str,
    ) -> Optional[BetfairSearchRequest]:
        """Map row to BetfairSearchRequest using MatchMapper"""
        try:
            return self.match_mapper.map_match_from_row(
                row=row,
                date_column=date_column,
                match_id_column=match_id_column,
                home_column=home_column,
                away_column=away_column,
                tournament_column=tournament_column,
            )
        except Exception as e:
            logger.error(f"Error in match mapping: {str(e)}")
            return None

    def _search_match(
        self, search_request: BetfairSearchRequest
    ) -> Optional[BetfairSearchResult]:
        """Search for match using BetfairSearchEngine"""
        try:
            return self.search_engine.search_main(search_request)
        except Exception as e:
            logger.error(f"Error in match search: {str(e)}")
            self.stats["search_failures"] += 1
            return None

    # new
    def _download_match_data(
        self, search_result: BetfairSearchResult
    ) -> Optional[Dict[str, Path]]:
        """Download match data with tracking"""
        try:
            # Use new tracking method
            downloaded_files, market_status = (
                self.downloader.download_odds_with_tracking(search_result)
            )

            # Update state manager with results
            if hasattr(self, "state_manager"):
                self.state_manager.update_match_downloads(
                    sofa_id=self.current_sofa_id, market_results=market_status
                )

            return downloaded_files if downloaded_files else None

        except Exception as e:
            logger.error(f"Error downloading match data: {str(e)}")
            return None

    def _extract_odds_data(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Extract odds data using MatchExtractor"""
        try:
            return self.extractor.extract_match(match_id)
        except Exception as e:
            logger.error(f"Error extracting odds data: {str(e)}")
            self.stats["extraction_failures"] += 1
            return None

    def _cleanup_temp_files(self, match_id: str) -> None:
        """Clean up temporary files for a match"""
        try:
            success = self.tmp_storage.cleanup_temp_match_folder(match_id)
            if success:
                logger.debug(f"Cleaned up temp files for match {match_id}")
            else:
                logger.warning(f"Failed to cleanup temp files for match {match_id}")
        except Exception as e:
            logger.warning(f"Error during cleanup for match {match_id}: {str(e)}")

    def _create_failed_result(
        self,
        sofa_match_id: int,
        status: ProcessingStatus,
        error_message: str,
        search_request: Optional[BetfairSearchRequest] = None,
        search_result: Optional[BetfairSearchResult] = None,
        downloaded_files: Optional[Dict[str, Path]] = None,
        betfair_match_id: Optional[str] = None,
        home_team_mapped: Optional[str] = None,
        away_team_mapped: Optional[str] = None,
        markets_found: Optional[List[str]] = None,
        markets_downloaded: Optional[List[str]] = None,
    ) -> ProcessingResult:
        """Create a failed processing result with metadata"""

        # Update failure statistics
        if status == ProcessingStatus.MAPPING_FAILED:
            self.stats["mapping_failures"] += 1
        elif status == ProcessingStatus.SEARCH_FAILED:
            self.stats["search_failures"] += 1
        elif status == ProcessingStatus.DOWNLOAD_FAILED:
            self.stats["download_failures"] += 1
        elif status == ProcessingStatus.EXTRACTION_FAILED:
            self.stats["extraction_failures"] += 1

        logger.warning(
            f"Match {sofa_match_id} failed at {status.value}: {error_message}"
        )

        return ProcessingResult(
            sofa_match_id=sofa_match_id,
            status=status,
            error_message=error_message,
            search_request=search_request,
            search_result=search_result,
            downloaded_files=downloaded_files,
            betfair_match_id=betfair_match_id,
            home_team_mapped=home_team_mapped,
            away_team_mapped=away_team_mapped,
            markets_found=markets_found or [],
            markets_downloaded=markets_downloaded or [],
        )

    def process_dataframe(
        self,
        df: pd.DataFrame,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team_slug",
        away_column: str = "away_team_slug",
        tournament_column: str = "tournament_id",
        cleanup_on_success: bool = True,
        cleanup_on_failure: bool = False,
        max_workers: Optional[int] = None,
    ) -> List[ProcessingResult]:
        """
        Process multiple matches from a DataFrame

        Args:
            df: DataFrame containing match information
            date_column: Name of the date column
            match_id_column: Name of match ID column
            home_column: Name of home team column
            away_column: Name of away team column
            tournament_column: Name of tournament ID column
            cleanup_on_success: Whether to cleanup temp files after successful processing
            cleanup_on_failure: Whether to cleanup temp files after failed processing
            max_workers: Maximum number of workers for parallel processing (None = sequential)

        Returns:
            List of ProcessingResults
        """
        from tqdm import tqdm

        logger.info(f"Starting batch processing of {len(df)} matches")

        results = []

        # Sequential processing with progress bar
        with tqdm(total=len(df), desc="Processing matches", unit="match") as pbar:
            for idx, row in df.iterrows():
                try:
                    result = self.process_from_row(
                        row=row,
                        date_column=date_column,
                        match_id_column=match_id_column,
                        home_column=home_column,
                        away_column=away_column,
                        tournament_column=tournament_column,
                        cleanup_on_success=cleanup_on_success,
                        cleanup_on_failure=cleanup_on_failure,
                    )
                    results.append(result)

                    # Update progress bar with current stats
                    pbar.set_postfix(
                        {
                            "success": self.stats["successful"],
                            "failed": self.stats["processed"]
                            - self.stats["successful"],
                            "current": result.sofa_match_id,
                        }
                    )

                except Exception as e:
                    logger.error(f"Unexpected error processing row {idx}: {str(e)}")
                    # Create a generic failure result
                    results.append(
                        ProcessingResult(
                            sofa_match_id=-1,
                            status=ProcessingStatus.UNEXPECTED_ERROR,
                            error_message=f"Row processing error: {str(e)}",
                        )
                    )

                pbar.update(1)

        self._log_batch_summary(results)
        return results

    def _log_batch_summary(self, results: List[ProcessingResult]) -> None:
        """Log summary of batch processing results"""
        total = len(results)
        successful = sum(1 for r in results if r.is_success)

        status_counts = {}
        for result in results:
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        logger.info(f"Batch processing complete: {successful}/{total} successful")
        logger.info(f"Status breakdown: {status_counts}")
        logger.info(f"Overall statistics: {self.stats}")

    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        total_processed = self.stats["processed"]
        success_rate = (
            (self.stats["successful"] / total_processed * 100)
            if total_processed > 0
            else 0
        )

        return {
            **self.stats,
            "success_rate_percent": round(success_rate, 2),
            "total_failures": total_processed - self.stats["successful"],
        }

    def reset_statistics(self) -> None:
        """Reset processing statistics"""
        self.stats = {key: 0 for key in self.stats}
        logger.info("Processing statistics reset")
