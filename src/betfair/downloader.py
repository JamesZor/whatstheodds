import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

from src.storage.temp_manager import TempStorage
from src.utils import load_config

from .api_client import setup_betfair_api_client
from .dataclasses import (
    BetfairSearchRequest,
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)

logger = logging.getLogger(__name__)


class BetfairDownloadError(Exception):
    """Custom exception for download-related errors"""

    pass


class BetfairDownloader:
    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        tmp_manager: Optional[TempStorage] = None,
        cfg: Optional[DictConfig] = None,
    ):
        self.cfg: DictConfig = load_config() if cfg is None else cfg
        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )
        self.tmp_storage: TempStorage = (
            TempStorage(cfg=self.cfg) if tmp_manager is None else tmp_manager
        )

    def _validate_search_result(
        self, search_result: BetfairSearchSingleMarketResult
    ) -> None:
        """Validate that search result has required fields for download"""
        if not search_result.file:
            raise ValueError(
                f"No file path in search result for market {search_result.market_type}"
            )

        if not search_result.match_id:
            raise ValueError(
                f"No match_id in search result for market {search_result.market_type}"
            )

        if not search_result.market_id:
            raise ValueError(
                f"No market_id in search result for market {search_result.market_type}"
            )

    def _download_single_market(
        self, search_result: BetfairSearchSingleMarketResult
    ) -> Path:
        """
        Download a single market file

        Args:
            search_result: Search result containing file path and metadata

        Returns:
            Path to the downloaded file

        Raises:
            ValueError: If search result is invalid
            BetfairDownloadError: If download fails
        """
        self._validate_search_result(search_result)

        # Ensure temp folder exists
        save_folder: Path = self.tmp_storage.get_match_folder_path(
            search_result.match_id
        )
        if not save_folder.exists():
            logger.warning(f"Match folder doesn't exist, creating: {save_folder}")
            self.tmp_storage.create_temp_match_folder(search_result.match_id)

        try:
            logger.info(
                f"Downloading {search_result.market_type} for match {search_result.match_id}"
            )

            # Perform the download using the API
            result = self.client.historic.download_file(
                file_path=search_result.file,
                store_directory=str(save_folder),
            )

            # Verify download success
            if not result:
                raise BetfairDownloadError(
                    "Download failed - no data received from API"
                )

            # Try to find the downloaded file
            downloaded_file = self._find_downloaded_file(save_folder, search_result)
            if not downloaded_file.exists():
                raise BetfairDownloadError(
                    f"Download completed but file not found: {downloaded_file}"
                )

            logger.info(
                f"Successfully downloaded {search_result.market_type} to {downloaded_file}"
            )
            return downloaded_file

        except betfairlightweight.exceptions.BetfairError as e:
            error_msg = (
                f"Betfair API error downloading {search_result.market_type}: {str(e)}"
            )
            logger.error(error_msg)
            raise BetfairDownloadError(error_msg) from e
        except Exception as e:
            error_msg = (
                f"Unexpected error downloading {search_result.market_type}: {str(e)}"
            )
            logger.error(error_msg)
            raise BetfairDownloadError(error_msg) from e

    def _find_downloaded_file(
        self, save_folder: Path, search_result: BetfairSearchSingleMarketResult
    ) -> Path:
        """Find the downloaded file in the save folder"""
        # The file should be named after the original file path
        original_filename = Path(search_result.file).name
        return save_folder / original_filename

    def download_odds(self, match: BetfairSearchResult) -> Dict[str, Path]:
        """
        Download all valid market files for a match

        Args:
            match: Search result containing valid markets to download

        Returns:
            Dict mapping market_type to downloaded file path

        Raises:
            ValueError: If match has no match_id
            BetfairDownloadError: If any download fails
        """
        if not match.match_id:
            raise ValueError("Cannot download odds: match has no match_id")

        if not match.valid_markets:
            logger.warning(f"No valid markets to download for match {match.match_id}")
            return {}

        # Create temp folder for the match
        try:
            self.tmp_storage.create_temp_match_folder(match_id=match.match_id)
        except Exception as e:
            raise BetfairDownloadError(
                f"Failed to create temp folder for match {match.match_id}: {e}"
            )

        downloaded_files: Dict[str, Path] = {}
        failed_downloads: List[str] = []

        # Download each valid market with progress bar
        with tqdm(
            total=len(match.valid_markets),
            desc=f"Downloading markets for match {match.match_id}",
            unit="market",
        ) as pbar:

            for market_type, search_result in match.valid_markets.items():
                # Update progress bar description to show current market
                pbar.set_description(f"Downloading {market_type}")

                try:
                    downloaded_file = self._download_single_market(search_result)
                    downloaded_files[market_type] = downloaded_file
                    pbar.set_postfix(status="✓ Success", refresh=False)
                except Exception as e:
                    logger.error(f"Failed to download {market_type}: {e}")
                    failed_downloads.append(market_type)
                    pbar.set_postfix(status="✗ Failed", refresh=False)

                # Update progress
                pbar.update(1)

        # Log results
        if downloaded_files:
            logger.info(
                f"Successfully downloaded {len(downloaded_files)} markets for match {match.match_id}"
            )

        if failed_downloads:
            error_msg = f"Failed to download {len(failed_downloads)} markets: {failed_downloads}"
            logger.error(error_msg)

            # If all downloads failed, cleanup and raise error
            if not downloaded_files:
                self.tmp_storage.cleanup_temp_match_folder(match.match_id)
                raise BetfairDownloadError(
                    f"All downloads failed for match {match.match_id}"
                )

        return downloaded_files

    def download_missing_markets(
        self, match: BetfairSearchResult, retry_search_func: Optional[Callable] = None
    ) -> Dict[str, Path]:
        """
        Attempt to download missing markets using a retry strategy

        Args:
            match: Search result with missing markets
            retry_search_func: Optional function to retry search for missing markets

        Returns:
            Dict of successfully downloaded missing markets
        """
        if not match.missing_markets:
            logger.info("No missing markets to retry")
            return {}

        downloaded_files: Dict[str, Path] = {}

        if retry_search_func:
            logger.info(
                f"Retrying search for {len(match.missing_markets)} missing markets"
            )
            # This would need to be implemented based on your retry strategy
            # For now, just log the missing markets

        for market_type, search_result in match.missing_markets.items():
            logger.warning(f"Missing market {market_type}: {search_result.error}")

        return downloaded_files

    def verify_downloads(self, match: BetfairSearchResult) -> Dict[str, bool]:
        """
        Verify that all expected files were downloaded successfully

        Returns:
            Dict mapping market_type to verification status
        """
        if not match.match_id:
            return {}

        verification_results: Dict[str, bool] = {}
        match_folder = self.tmp_storage.get_match_folder_path(match.match_id)

        if not match_folder.exists():
            logger.error(f"Match folder not found: {match_folder}")
            return {market: False for market in match.valid_markets.keys()}

        for market_type, search_result in match.valid_markets.items():
            try:
                expected_file = self._find_downloaded_file(match_folder, search_result)
                verification_results[market_type] = (
                    expected_file.exists() and expected_file.stat().st_size > 0
                )
            except Exception as e:
                logger.error(f"Error verifying {market_type}: {e}")
                verification_results[market_type] = False

        return verification_results

    def get_download_summary(self, match: BetfairSearchResult) -> Dict[str, Any]:
        """
        Get summary of download status for a match

        Returns:
            Dict with download statistics and file information
        """
        if not match.match_id:
            return {"error": "No match_id"}

        match_folder = self.tmp_storage.get_match_folder_path(match.match_id)
        verification_results = self.verify_downloads(match)

        total_expected = len(match.valid_markets)
        downloaded_count = sum(verification_results.values())
        folder_size = self.tmp_storage.get_temp_folder_size(match.match_id)

        return {
            "match_id": match.match_id,
            "total_expected_files": total_expected,
            "successfully_downloaded": downloaded_count,
            "missing_markets": len(match.missing_markets),
            "download_success_rate": (
                downloaded_count / total_expected if total_expected > 0 else 0
            ),
            "folder_exists": match_folder.exists(),
            "total_folder_size_bytes": folder_size,
            "verification_details": verification_results,
        }

    def cleanup_failed_download(self, match_id: str) -> bool:
        """Clean up files from a failed download"""
        try:
            return self.tmp_storage.cleanup_temp_match_folder(match_id)
        except Exception as e:
            logger.error(f"Failed to cleanup after failed download for {match_id}: {e}")
            return False
