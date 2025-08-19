import bz2
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.storage.temp_manager import TempStorage
from whatstheodds.utils import load_config

from .api_client import setup_betfair_api_client
from .dataclasses import (
    BetfairSearchRequest,
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)

logger = logging.getLogger(__name__)
from .rate_limiter import RateLimitedContext, betfair_rate_limiter

logger.setLevel(level=logging.DEBUG)


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
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]
        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )
        self.tmp_storage: TempStorage = (
            TempStorage(cfg=self.cfg) if tmp_manager is None else tmp_manager
        )

        # Track authentication state
        self._auth_attempts = 0
        self._max_auth_attempts = self.cfg.betfair_downloader.max_auth_attempts

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

    def _validate_downloaded_file(self, file_path: Path) -> bool:
        """
        Validate that the downloaded file is compressed data, not HTML

        Args:
            file_path: Path to the downloaded file

        Returns:
            True if valid compressed data, False if HTML or invalid
        """
        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            return False

        # Check file size - HTML error pages are typically small
        file_size = file_path.stat().st_size
        if (
            file_size < self.cfg.betfair_downloader.error_file_size
        ):  # Less than 1KB is suspicious
            logger.warning(
                f"File size too small ({file_size} bytes), likely an error page"
            )
            return False

        try:
            # Try to read the file as compressed data
            with bz2.open(file_path, "rt", encoding="utf-8") as f:
                first_line = f.readline().strip()

                # Check if it's HTML
                if any(
                    html_marker in first_line.lower()
                    for html_marker in ["<!doctype", "<html", "<?xml"]
                ):
                    logger.error(f"File contains HTML instead of data: {file_path}")

                    # Log the first few lines for debugging
                    with open(file_path, "r", encoding="utf-8") as html_file:
                        html_content = html_file.read(500)
                        logger.debug(f"HTML content (first 500 chars): {html_content}")

                    return False

                # Try to parse as JSON (Betfair data should be JSON lines)
                try:
                    json.loads(first_line)
                    logger.debug(f"File validated as compressed JSON data: {file_path}")
                    return True
                except json.JSONDecodeError:
                    logger.error(f"File is not valid JSON: {first_line[:100]}")
                    return False

        except (IOError, UnicodeDecodeError) as e:
            # File might not be compressed or might be corrupted
            logger.error(f"Error reading file {file_path}: {str(e)}")

            # Check if it's uncompressed HTML
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read(100)
                    if any(
                        marker in content.lower() for marker in ["<!doctype", "<html"]
                    ):
                        logger.error("File is uncompressed HTML")
                        return False
            except:
                pass

            return False

    def _reauthenticate(self) -> bool:
        """
        Attempt to reauthenticate with Betfair

        Returns:
            True if successful, False otherwise
        """
        if self._auth_attempts >= self._max_auth_attempts:
            logger.error(
                f"Max authentication attempts ({self._max_auth_attempts}) reached"
            )
            return False

        try:
            self._auth_attempts += 1
            logger.info(
                f"Reauthenticating (attempt {self._auth_attempts}/{self._max_auth_attempts})"
            )

            # Logout first if there's an existing session
            if hasattr(self.client, "session_token") and self.client.session_token:
                try:
                    self.client.logout()
                except:
                    pass  # Ignore logout errors

            # Re-login
            self.client.login_interactive()
            logger.info("Successfully reauthenticated")

            # Reset counter on success
            self._auth_attempts = 0
            return True

        except Exception as e:
            logger.error(f"Reauthentication failed: {str(e)}")
            return False

    def _attempt_download(self, search_result, save_folder):
        """Single download attempt - returns Path if successful, None if failed"""
        try:
            with RateLimitedContext():
                self.client.historic.download_file(
                    file_path=search_result.file,
                    store_directory=str(save_folder),
                )

            downloaded_file = self._find_downloaded_file(save_folder, search_result)

            if self._validate_downloaded_file(downloaded_file):
                return downloaded_file
            else:
                downloaded_file.unlink(missing_ok=True)
                return None

        except Exception as e:
            logger.warning(f"Download attempt failed: {e}")
            return None

    def _download_with_retries(self, search_result, save_folder, retry_on_html):
        """Download with built-in retry logic"""

        # Attempt 1: Normal download
        result = self._attempt_download(search_result, save_folder)
        if result:
            return result

        if not retry_on_html:
            raise BetfairDownloadError(
                f"Download failed for {search_result.market_type}"
            )

        # Attempt 2: Simple retry (for 302s)
        # Get config values with sensible defaults
        max_302_retries = getattr(self.cfg, "betfair_downloader", {}).get(
            "html_retries", 3
        )
        sleep_between_retries = getattr(self.cfg, "betfair_downloader", {}).get(
            "html_sleep_before_retry", 3
        )

        for attempt in range(max_302_retries):
            logger.info(
                f"302 retry attempt {attempt + 1}/{max_302_retries} for {search_result.market_type}"
            )
            time.sleep(sleep_between_retries)

            result = self._attempt_download(search_result, save_folder)
            if result:
                logger.info(
                    f"302 retry worked on attempt {attempt + 1} for {search_result.market_type}"
                )
                return result

            logger.warning(
                f"302 retry attempt {attempt + 1} failed for {search_result.market_type}"
            )

        # Attempt 3: Reauthenticate and retry
        logger.warning(
            f"Simple retry failed, reauthenticating for {search_result.market_type}"
        )
        if self._reauthenticate():
            result = self._attempt_download(search_result, save_folder)
            if result:
                logger.info(f"✅ Auth retry worked for {search_result.market_type}")
                return result

        # All attempts failed
        raise BetfairDownloadError(
            f"All retry attempts failed for {search_result.market_type}"
        )

    def _download_single_market(self, search_result, retry_on_html=True):
        """
        Download a single market file with validation

        Args:
            search_result: Search result containing file path and metadata
            retry_on_html: Whether to retry with reauthentication if HTML is received

        Returns:
            Path to the downloaded file

        Raises:
            ValueError: If search result is invalid
            BetfairDownloadError: If download fails
        """
        save_folder = self.tmp_storage.get_match_folder_path(search_result.match_id)

        # Try the download with automatic retries
        return self._download_with_retries(search_result, save_folder, retry_on_html)

    # def _download_single_market(
    #     self, search_result: BetfairSearchSingleMarketResult, retry_on_html: bool = True
    # ) -> Path:
    #     """
    #     Download a single market file with validation
    #
    #     Args:
    #         search_result: Search result containing file path and metadata
    #         retry_on_html: Whether to retry with reauthentication if HTML is received
    #
    #     Returns:
    #         Path to the downloaded file
    #
    #     Raises:
    #         ValueError: If search result is invalid
    #         BetfairDownloadError: If download fails
    #     """
    #     self._validate_search_result(search_result)
    #
    #     # Ensure temp folder exists
    #     save_folder: Path = self.tmp_storage.get_match_folder_path(
    #         search_result.match_id
    #     )
    #     if not save_folder.exists():
    #         logger.warning(f"Match folder doesn't exist, creating: {save_folder}")
    #         self.tmp_storage.create_temp_match_folder(search_result.match_id)
    #
    #     try:
    #         logger.info(
    #             f"Downloading {search_result.market_type} for match {search_result.match_id}"
    #         )
    #
    #         # Perform the download using the API
    #         logger.info(
    #             f"About to make download API call for {search_result.market_type}"
    #         )
    #         with RateLimitedContext():
    #             logger.info("Rate limiter cleared, making actual API call")
    #             result = self.client.historic.download_file(
    #                 file_path=search_result.file,
    #                 store_directory=str(save_folder),
    #             )
    #
    #         logger.info(f"API call completed for {search_result.market_type}")
    #         # Verify download success
    #         if not result:
    #             raise BetfairDownloadError(
    #                 "Download failed - no data received from API"
    #             )
    #
    #         # Find the downloaded file
    #         downloaded_file: Path = self._find_downloaded_file(
    #             save_folder, search_result
    #         )
    #
    #         # Validate the downloaded file
    #         if not self._validate_downloaded_file(downloaded_file):
    #             # Delete the invalid file
    #             downloaded_file.unlink(missing_ok=True)
    #
    #             # If we got HTML, it's likely an auth issue
    #             if retry_on_html:
    #                 logger.warning(
    #                     "Received HTML instead of data, attempting reauthentication"
    #                 )
    #                 logger.info(
    #                     f"Trying simple retry for {search_result.market_type} (might be temporary 302 redirect)"
    #                 )
    #                 time.sleep(self.cfg.betfair_downloader.html_sleep_retry)
    #
    #                 try:
    #                     # Try the download one more time without reauthentication
    #                     with RateLimitedContext():
    #                         retry_result = self.client.historic.download_file(
    #                             file_path=search_result.file,
    #                             store_directory=str(save_folder),
    #                         )
    #
    #                     if retry_result:
    #                         retry_downloaded_file = self._find_downloaded_file(
    #                             save_folder, search_result
    #                         )
    #                         if self._validate_downloaded_file(retry_downloaded_file):
    #                             logger.info(
    #                                 f"Simple retry worked for {search_result.market_type}!"
    #                             )
    #                             return retry_downloaded_file
    #                         else:
    #                             # Clean up the failed retry file
    #                             retry_downloaded_file.unlink(missing_ok=True)
    #                             logger.warning(
    #                                 f"Simple retry failed for {search_result.market_type}, trying reauthentication..."
    #                             )
    #                     else:
    #                         logger.warning(
    #                             f"Simple retry got no result for {search_result.market_type}"
    #                         )
    #
    #                 except Exception as retry_error:
    #                     logger.warning(
    #                         f"Simple retry errored for {search_result.market_type}: {retry_error}"
    #                     )
    #
    #                 if self._reauthenticate():
    #                     # Retry the download once after reauthentication
    #                     return self._download_single_market(
    #                         search_result, retry_on_html=False
    #                     )
    #                 else:
    #                     raise BetfairDownloadError(
    #                         f"Download failed for {search_result.market_type}: "
    #                         "Received HTML page (likely authentication issue)"
    #                     )
    #             else:
    #                 raise BetfairDownloadError(
    #                     f"Download validation failed for {search_result.market_type}: "
    #                     "Invalid file format after retry"
    #                 )
    #
    #         logger.info(
    #             f"Successfully downloaded and validated {search_result.market_type} to {downloaded_file}"
    #         )
    #         return downloaded_file
    #
    #     except betfairlightweight.exceptions.BetfairError as e:
    #         error_msg = (
    #             f"Betfair API error downloading {search_result.market_type}: {str(e)}"
    #         )
    #         logger.error(error_msg)
    #
    #         # Check if it's an auth error
    #         if "session" in str(e).lower() or "login" in str(e).lower():
    #             if retry_on_html and self._reauthenticate():
    #                 return self._download_single_market(
    #                     search_result, retry_on_html=False
    #                 )
    #
    #         raise BetfairDownloadError(error_msg) from e
    #
    #     except Exception as e:
    #         error_msg = (
    #             f"Unexpected error downloading {search_result.market_type}: {str(e)}"
    #         )
    #         logger.error(error_msg)
    #         raise BetfairDownloadError(error_msg) from e
    #
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
                # Use our validation method instead of just checking existence
                verification_results[market_type] = self._validate_downloaded_file(
                    expected_file
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

        # Check for HTML files (failed downloads)
        html_files = []
        if match_folder.exists():
            for file_path in match_folder.glob("*.bz2"):
                if not self._validate_downloaded_file(file_path):
                    html_files.append(file_path.name)

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
            "invalid_html_files": html_files,
            "authentication_attempts": self._auth_attempts,
        }

    def cleanup_failed_download(self, match_id: str) -> bool:
        """Clean up files from a failed download"""
        try:
            return self.tmp_storage.cleanup_temp_match_folder(match_id)
        except Exception as e:
            logger.error(f"Failed to cleanup after failed download for {match_id}: {e}")
            return False


###
