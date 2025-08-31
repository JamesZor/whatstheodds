"""
Tests for enhanced BetfairDownloader with state tracking
"""

import bz2
import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from whatstheodds.betfair.dataclasses import (
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)
from whatstheodds.betfair.downloader_enhanced import BetfairDownloader


@pytest.fixture
def temp_storage_dir(tmp_path):
    """Create temporary storage directory"""
    storage_dir = tmp_path / "temp_storage"
    storage_dir.mkdir()
    return storage_dir


@pytest.fixture
def mock_api_client():
    """Create mock Betfair API client"""
    client = Mock()
    client.historic = Mock()
    client.historic.download_file = Mock(return_value=True)
    return client


@pytest.fixture
def mock_temp_storage(temp_storage_dir):
    """Create mock TempStorage"""
    storage = Mock()
    storage.get_match_folder_path = Mock(return_value=temp_storage_dir / "match_folder")
    storage.create_temp_match_folder = Mock()
    storage.cleanup_temp_match_folder = Mock(return_value=True)
    storage.get_temp_folder_size = Mock(return_value=1024)
    return storage


@pytest.fixture
def sample_search_result():
    """Create sample BetfairSearchResult"""
    return BetfairSearchResult(
        match_id="31679991",
        valid_markets={
            "MATCH_ODDS": BetfairSearchSingleMarketResult(
                strategy_used="ExactDateTeamSearch",
                market_type="MATCH_ODDS",
                file="/path/to/match_odds.bz2",
                market_id="1.202358758",
                match_id="31679991",
            ),
            "OVER_UNDER_25": BetfairSearchSingleMarketResult(
                strategy_used="ExactDateTeamSearch",
                market_type="OVER_UNDER_25",
                file="/path/to/over_under.bz2",
                market_id="1.202358759",
                match_id="31679991",
            ),
            "CORRECT_SCORE": BetfairSearchSingleMarketResult(
                strategy_used="ExactDateTeamSearch",
                market_type="CORRECT_SCORE",
                file="/path/to/correct_score.bz2",
                market_id="1.202358760",
                match_id="31679991",
            ),
        },
        missing_markets={},
    )


@pytest.fixture
def downloader(mock_api_client, mock_temp_storage):
    """Create BetfairDownloader instance with mocks"""
    return BetfairDownloader(api_client=mock_api_client, tmp_manager=mock_temp_storage)


def create_mock_bz2_file(filepath: Path, content: Dict[str, Any] = None):
    """Create a mock compressed file with JSON content"""
    if content is None:
        content = {"test": "data", "odds": [1.5, 2.0, 3.5]}

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with bz2.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(content, f)


class TestEnhancedDownloaderMethods:
    """Tests for new enhanced methods"""

    def test_get_existing_markets_empty(self, downloader, temp_storage_dir):
        """Test getting existing markets when folder doesn't exist"""
        downloader.tmp_storage.get_match_folder_path.return_value = (
            temp_storage_dir / "nonexistent"
        )

        existing = downloader.get_existing_markets("31679991")

        assert existing == {}

    def test_get_existing_markets_with_files(self, downloader, temp_storage_dir):
        """Test getting existing markets with downloaded files"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        # Create mock downloaded files
        create_mock_bz2_file(match_folder / "match_odds.bz2")
        create_mock_bz2_file(match_folder / "over_under.bz2")

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        existing = downloader.get_existing_markets("31679991")

        assert len(existing) == 2
        assert match_folder / "match_odds.bz2" in existing.values()
        assert match_folder / "over_under.bz2" in existing.values()

    def test_get_existing_markets_filters_invalid(self, downloader, temp_storage_dir):
        """Test that invalid files are filtered out"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        # Create valid file
        create_mock_bz2_file(match_folder / "match_odds.bz2")

        # Create invalid file (HTML error)
        invalid_file = match_folder / "error.bz2"
        with open(invalid_file, "w") as f:
            f.write("<!DOCTYPE html><html>Error</html>")

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        existing = downloader.get_existing_markets("31679991")

        # Should only include valid file
        assert len(existing) == 1
        assert match_folder / "match_odds.bz2" in existing.values()
        assert invalid_file not in existing.values()

    def test_download_selective_markets_all_new(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test downloading selective markets when none exist"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        # Mock successful downloads
        def mock_download(file_path, store_directory):
            filename = Path(file_path).name
            create_mock_bz2_file(Path(store_directory) / filename)
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        # Download specific markets
        markets_to_download = ["MATCH_ODDS", "OVER_UNDER_25"]
        downloaded = downloader.download_selective_markets(
            sample_search_result, markets_to_download
        )

        assert len(downloaded) == 2
        assert "MATCH_ODDS" in downloaded
        assert "OVER_UNDER_25" in downloaded
        assert "CORRECT_SCORE" not in downloaded

        # Verify API was called for each market
        assert downloader.client.historic.download_file.call_count == 2

    def test_download_selective_markets_skip_existing(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test that existing markets are skipped"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        # Create existing file
        existing_file = match_folder / "match_odds.bz2"
        create_mock_bz2_file(existing_file)

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        # Mock download for new files
        def mock_download(file_path, store_directory):
            filename = Path(file_path).name
            if "over_under" in filename:
                create_mock_bz2_file(Path(store_directory) / filename)
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        # Try to download both markets
        markets_to_download = ["MATCH_ODDS", "OVER_UNDER_25"]
        downloaded = downloader.download_selective_markets(
            sample_search_result, markets_to_download, skip_existing=True
        )

        # Should have both markets (one existing, one new)
        assert len(downloaded) == 2
        assert downloaded["MATCH_ODDS"] == existing_file  # Existing file
        assert "OVER_UNDER_25" in downloaded

        # API should only be called once (for OVER_UNDER_25)
        assert downloader.client.historic.download_file.call_count == 1

    def test_download_selective_markets_with_failures(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test handling of download failures"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        # Mock mixed success/failure
        def mock_download(file_path, store_directory):
            if "match_odds" in file_path:
                # Success
                create_mock_bz2_file(Path(store_directory) / Path(file_path).name)
                return True
            else:
                # Failure - create HTML error file
                error_file = Path(store_directory) / Path(file_path).name
                with open(error_file, "w") as f:
                    f.write("<!DOCTYPE html><html>Error</html>")
                return True

        downloader.client.historic.download_file.side_effect = mock_download

        markets_to_download = ["MATCH_ODDS", "OVER_UNDER_25"]
        downloaded = downloader.download_selective_markets(
            sample_search_result, markets_to_download, raise_on_failure=False
        )

        # Only successful download should be included
        assert len(downloaded) == 1
        assert "MATCH_ODDS" in downloaded
        assert "OVER_UNDER_25" not in downloaded

    def test_download_odds_with_tracking(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test download_odds_with_tracking returns files and status"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder
        downloader.tmp_storage.create_temp_match_folder.return_value = None

        # Mock downloads with one failure
        def mock_download(file_path, store_directory):
            filename = Path(file_path).name
            if "correct_score" in filename:
                # Simulate failure
                error_file = Path(store_directory) / filename
                with open(error_file, "w") as f:
                    f.write("<!DOCTYPE html><html>Error</html>")
            else:
                # Success
                create_mock_bz2_file(Path(store_directory) / filename)
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        # Download with tracking
        downloaded_files, market_status = downloader.download_odds_with_tracking(
            sample_search_result
        )

        # Check downloaded files
        assert len(downloaded_files) == 2
        assert "MATCH_ODDS" in downloaded_files
        assert "OVER_UNDER_25" in downloaded_files
        assert "CORRECT_SCORE" not in downloaded_files

        # Check market status
        assert market_status["MATCH_ODDS"] == "success"
        assert market_status["OVER_UNDER_25"] == "success"
        assert market_status["CORRECT_SCORE"] == "failed"

    def test_download_odds_with_tracking_all_failed(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test tracking when all downloads fail"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        # Mock all failures
        def mock_download(file_path, store_directory):
            # Create HTML error files
            error_file = Path(store_directory) / Path(file_path).name
            with open(error_file, "w") as f:
                f.write("<!DOCTYPE html><html>Error</html>")
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        downloaded_files, market_status = downloader.download_odds_with_tracking(
            sample_search_result
        )

        # No successful downloads
        assert len(downloaded_files) == 0

        # All marked as failed
        assert all(status == "failed" for status in market_status.values())
        assert len(market_status) == 3

    def test_download_odds_with_tracking_empty_search(self, downloader):
        """Test handling empty search result"""
        empty_result = BetfairSearchResult(
            match_id="31679991", valid_markets={}, missing_markets={}
        )

        downloaded_files, market_status = downloader.download_odds_with_tracking(
            empty_result
        )

        assert downloaded_files == {}
        assert market_status == {}

    def test_download_selective_markets_invalid_market(
        self, downloader, sample_search_result
    ):
        """Test requesting invalid market type"""
        markets_to_download = ["MATCH_ODDS", "INVALID_MARKET"]

        with pytest.raises(ValueError, match="Market INVALID_MARKET not found"):
            downloader.download_selective_markets(
                sample_search_result, markets_to_download, raise_on_invalid=True
            )

    def test_download_selective_markets_skip_invalid(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test skipping invalid market types"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        def mock_download(file_path, store_directory):
            create_mock_bz2_file(Path(store_directory) / Path(file_path).name)
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        markets_to_download = ["MATCH_ODDS", "INVALID_MARKET"]

        downloaded = downloader.download_selective_markets(
            sample_search_result, markets_to_download, raise_on_invalid=False
        )

        # Should only download valid market
        assert len(downloaded) == 1
        assert "MATCH_ODDS" in downloaded
        assert "INVALID_MARKET" not in downloaded

    def test_retry_failed_markets(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test retrying only failed markets"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        # Create existing successful file
        existing_file = match_folder / "match_odds.bz2"
        create_mock_bz2_file(existing_file)

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        # Mock successful retry
        def mock_download(file_path, store_directory):
            create_mock_bz2_file(Path(store_directory) / Path(file_path).name)
            return True

        downloader.client.historic.download_file.side_effect = mock_download

        # Retry only failed markets
        failed_markets = ["OVER_UNDER_25", "CORRECT_SCORE"]
        downloaded = downloader.retry_failed_markets(
            sample_search_result, failed_markets
        )

        assert len(downloaded) == 2
        assert "OVER_UNDER_25" in downloaded
        assert "CORRECT_SCORE" in downloaded

        # Should not re-download MATCH_ODDS
        assert downloader.client.historic.download_file.call_count == 2

    def test_get_market_status_summary(
        self, downloader, sample_search_result, temp_storage_dir
    ):
        """Test getting summary of market download status"""
        match_folder = temp_storage_dir / "31679991"
        match_folder.mkdir()

        # Create files with different statuses
        create_mock_bz2_file(match_folder / "match_odds.bz2")

        # Invalid file
        with open(match_folder / "over_under.bz2", "w") as f:
            f.write("<!DOCTYPE html>Error</html>")

        downloader.tmp_storage.get_match_folder_path.return_value = match_folder

        summary = downloader.get_market_status_summary("31679991", sample_search_result)

        assert summary["match_id"] == "31679991"
        assert summary["total_markets"] == 3
        assert summary["downloaded_markets"] == 1
        assert summary["failed_markets"] == 1
        assert summary["not_attempted_markets"] == 1
        assert "MATCH_ODDS" in summary["successful"]
        assert "OVER_UNDER_25" in summary["failed"]
        assert "CORRECT_SCORE" in summary["not_attempted"]
