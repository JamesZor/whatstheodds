import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import betfairlightweight
import pandas as pd
from omegaconf import DictConfig

from src.betfair.api_client import setup_betfair_api_client
from src.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from src.betfair.downloader import BetfairDownloader
from src.betfair.search_engine import BetfairSearchEngine
from src.extractors import MatchExtractor
from src.mappers.match_mapper import MatchMapper
from src.storage.temp_manager import TempStorage
from src.utils import load_config

logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Enum for processing status tracking"""

    SUCCESS = "success"
    MAPPING_FAILED = "mapping_failed"
    SEARCH_FAILED = "search_failed"
    DOWNLOAD_FAILED = "download_failed"
    EXTRACTION_FAILED = "extraction_failed"
    UNEXPECTED_ERROR = "unexpected_error"


@dataclass
class ProcessingResult:
    """Result of processing a single match row"""

    sofa_match_id: int
    status: ProcessingStatus
    search_request: Optional[BetfairSearchRequest] = None
    search_result: Optional[BetfairSearchResult] = None
    downloaded_files: Optional[Dict[str, Path]] = None
    extracted_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    processing_time_seconds: Optional[float] = None

    # Additional metadata
    betfair_match_id: Optional[str] = None
    home_team_mapped: Optional[str] = None
    away_team_mapped: Optional[str] = None
    markets_found: List[str] = field(default_factory=list)
    markets_downloaded: List[str] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        """Check if processing was successful"""
        return self.status == ProcessingStatus.SUCCESS

    @property
    def has_odds_data(self) -> bool:
        """Check if odds data was extracted successfully"""
        return (
            self.is_success
            and self.extracted_data is not None
            and "odds_data" in self.extracted_data
        )


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
            MatchExtractor(config=self.cfg) if extractor is None else extractor
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

    def _download_match_data(
        self, search_result: BetfairSearchResult
    ) -> Optional[Dict[str, Path]]:
        """Download match data using BetfairDownloader"""
        try:
            return self.downloader.download_odds(search_result)
        except Exception as e:
            logger.error(f"Error downloading match data: {str(e)}")
            self.stats["download_failures"] += 1
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
