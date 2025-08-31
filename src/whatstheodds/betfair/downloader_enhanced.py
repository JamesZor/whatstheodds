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

    #### New adds

    def get_existing_markets(self, match_id: str) -> Dict[str, Path]:
        """
        Check which markets already exist in the temp folder

        Args:
            match_id: Betfair match ID

        Returns:
            Dict mapping market filename to Path of existing valid files
        """
        match_folder = self.tmp_storage.get_match_folder_path(match_id)

        if not match_folder.exists():
            logger.debug(f"Match folder doesn't exist: {match_folder}")
            return {}

        existing_markets = {}

        # Check all .bz2 files in the folder
        for file_path in match_folder.glob("*.bz2"):
            # Validate that it's a proper data file (not HTML error)
            if self._validate_downloaded_file(file_path):
                # Use filename without extension as key
                market_key = file_path.stem
                existing_markets[market_key] = file_path
                logger.debug(f"Found existing valid market file: {file_path}")
            else:
                logger.debug(f"Found invalid market file (skipping): {file_path}")

        logger.info(
            f"Found {len(existing_markets)} existing valid markets for match {match_id}"
        )
        return existing_markets

    # add 2
    def download_selective_markets(
        self,
        search_result: BetfairSearchResult,
        markets_to_download: List[str],
        skip_existing: bool = True,
        raise_on_failure: bool = False,
        raise_on_invalid: bool = False,
    ) -> Dict[str, Path]:
        """
        Download only specific markets from a search result

        Args:
            search_result: BetfairSearchResult with valid markets
            markets_to_download: List of market types to download
            skip_existing: If True, skip markets that already exist
            raise_on_failure: If True, raise exception on download failure
            raise_on_invalid: If True, raise exception if requested market not found

        Returns:
            Dict mapping market_type to downloaded file Path

        Raises:
            ValueError: If raise_on_invalid=True and market not found
            BetfairDownloadError: If raise_on_failure=True and download fails
        """
        if not search_result.match_id:
            raise ValueError("Cannot download: search result has no match_id")

        # Create temp folder if needed
        try:
            self.tmp_storage.create_temp_match_folder(match_id=search_result.match_id)
        except Exception as e:
            logger.warning(f"Folder creation issue (may already exist): {e}")

        downloaded_files = {}
        existing_markets = {}

        # Check existing markets if skip_existing is True
        if skip_existing:
            existing_markets = self.get_existing_markets(search_result.match_id)

        for market_type in markets_to_download:
            # Check if market exists in search result
            if market_type not in search_result.valid_markets:
                msg = f"Market {market_type} not found in search result"
                logger.warning(msg)
                if raise_on_invalid:
                    raise ValueError(msg)
                continue

            search_market = search_result.valid_markets[market_type]

            # Check if already exists
            if skip_existing:
                # Try to match by market type or filename
                existing_file = None
                for key, path in existing_markets.items():
                    # Check if the existing file matches this market
                    if (
                        market_type.lower() in key.lower()
                        or search_market.market_id in key
                    ):
                        existing_file = path
                        break

                if existing_file:
                    logger.info(
                        f"Skipping {market_type} - already exists at {existing_file}"
                    )
                    downloaded_files[market_type] = existing_file
                    continue

            # Download the market
            try:
                logger.info(f"Downloading market: {market_type}")
                downloaded_file = self._download_single_market(search_market)
                downloaded_files[market_type] = downloaded_file
                logger.info(f"Successfully downloaded {market_type}")

            except Exception as e:
                logger.error(f"Failed to download {market_type}: {e}")
                if raise_on_failure:
                    raise

        logger.info(
            f"Downloaded {len(downloaded_files)} markets (requested {len(markets_to_download)})"
        )
        return downloaded_files

    # add 3

    def download_odds_with_tracking(
        self, match: BetfairSearchResult
    ) -> Tuple[Dict[str, Path], Dict[str, str]]:
        """
        Download all valid markets and return both files and status tracking

        Args:
            match: BetfairSearchResult with valid markets

        Returns:
            Tuple of:
                - Dict mapping market_type to downloaded file Path
                - Dict mapping market_type to status ("success"/"failed"/"not_attempted")
        """
        if not match.match_id:
            logger.warning("Cannot download odds: match has no match_id")
            return {}, {}

        if not match.valid_markets:
            logger.warning(f"No valid markets to download for match {match.match_id}")
            return {}, {}

        # Create temp folder for the match
        try:
            self.tmp_storage.create_temp_match_folder(match_id=match.match_id)
        except Exception as e:
            logger.warning(f"Folder creation issue: {e}")

        downloaded_files = {}
        market_status = {}

        with tqdm(
            total=len(match.valid_markets),
            desc=f"Downloading markets for match {match.match_id}",
            unit="market",
        ) as pbar:

            # Download each valid market
            for market_type, search_result in match.valid_markets.items():
                pbar.set_description(f"Downloading {market_type}")

                try:
                    logger.info(f"Downloading {market_type} for match {match.match_id}")
                    downloaded_file = self._download_single_market(search_result)
                    # Validate the downloaded file
                    if self._validate_downloaded_file(downloaded_file):
                        downloaded_files[market_type] = downloaded_file
                        market_status[market_type] = "success"
                        logger.info(
                            f"✓ Successfully downloaded and validated {market_type}"
                        )
                        pbar.set_postfix(status=f"✓ {market_type}", refresh=False)
                    else:
                        # File downloaded but invalid (e.g., HTML error)
                        market_status[market_type] = "failed"
                        logger.error(f"✗ Downloaded file invalid for {market_type}")
                        # Clean up invalid file
                        downloaded_file.unlink(missing_ok=True)
                        pbar.set_postfix(status=f"✗ {market_type}", refresh=False)

                except Exception as e:
                    logger.error(f"✗ Failed to download {market_type}: {e}")
                    market_status[market_type] = "failed"

                pbar.update(1)

        # Mark missing markets as not_attempted
        for market_type in match.missing_markets:
            market_status[market_type] = "not_attempted"

        # Log summary
        success_count = sum(1 for s in market_status.values() if s == "success")
        failed_count = sum(1 for s in market_status.values() if s == "failed")

        logger.info(
            f"Download complete for match {match.match_id}: "
            f"{success_count} success, {failed_count} failed"
        )

        return downloaded_files, market_status

    # add 4

    def retry_failed_markets(
        self, search_result: BetfairSearchResult, failed_markets: List[str]
    ) -> Dict[str, Path]:
        """
        Retry downloading only the failed markets

        Args:
            search_result: Original search result with market information
            failed_markets: List of market types that failed

        Returns:
            Dict mapping market_type to newly downloaded file Path
        """
        logger.info(
            f"Retrying {len(failed_markets)} failed markets for match {search_result.match_id}"
        )

        # Use selective download to retry only failed markets
        return self.download_selective_markets(
            search_result=search_result,
            markets_to_download=failed_markets,
            skip_existing=False,  # Try to re-download even if file exists
            raise_on_failure=False,  # Don't raise, just log failures
        )

    # add 5
    def get_market_status_summary(
        self, match_id: str, search_result: Optional[BetfairSearchResult] = None
    ) -> Dict[str, Any]:
        """
        Get a summary of market download status for a match

        Args:
            match_id: Betfair match ID
            search_result: Optional search result for complete market list

        Returns:
            Dict with download status summary
        """
        existing_markets = self.get_existing_markets(match_id)
        match_folder = self.tmp_storage.get_match_folder_path(match_id)

        summary = {
            "match_id": match_id,
            "folder_exists": match_folder.exists(),
            "downloaded_markets": len(existing_markets),
            "successful": [],
            "failed": [],
            "not_attempted": [],
        }

        if search_result and search_result.valid_markets:
            summary["total_markets"] = len(search_result.valid_markets)

            # Check each market from search result
            for market_type in search_result.valid_markets:
                # Check if file exists and is valid
                found = False
                for key in existing_markets:
                    if market_type.lower() in key.lower():
                        summary["successful"].append(market_type)
                        found = True
                        break

                if not found:
                    # Check if there's an invalid file
                    match_folder = self.tmp_storage.get_match_folder_path(match_id)
                    for file_path in match_folder.glob("*.bz2"):
                        if market_type.lower() in file_path.stem.lower():
                            if not self._validate_downloaded_file(file_path):
                                summary["failed"].append(market_type)
                                found = True
                                break

                    if not found:
                        summary["not_attempted"].append(market_type)

            summary["failed_markets"] = len(summary["failed"])
            summary["not_attempted_markets"] = len(summary["not_attempted"])
        else:
            summary["total_markets"] = None
            summary["failed_markets"] = None
            summary["not_attempted_markets"] = None

        return summary

    # back to og
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
