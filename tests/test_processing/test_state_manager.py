"""
Tests for the ProcessingStateManager and MatchState classes
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pytest

from whatstheodds.processor.state_manager import (
    MatchState,
    ProcessingStateManager,
    StateVersion,
)


@pytest.fixture
def temp_state_dir(tmp_path):
    """Create a temporary directory for state files"""
    state_dir = tmp_path / "state_files"
    state_dir.mkdir()
    return state_dir


@pytest.fixture
def sample_search_result():
    """Create a sample search result dictionary"""
    return {
        "match_id": "31679991",
        "valid_markets": {
            "MATCH_ODDS": {
                "strategy_used": "ExactDateTeamSearch",
                "market_type": "MATCH_ODDS",
                "file": "/path/to/file.bz2",
                "market_id": "1.202358758",
                "match_id": "31679991",
            },
            "OVER_UNDER_25": {
                "strategy_used": "ExactDateTeamSearch",
                "market_type": "OVER_UNDER_25",
                "file": "/path/to/file2.bz2",
                "market_id": "1.202358759",
                "match_id": "31679991",
            },
        },
        "missing_markets": {},
    }


@pytest.fixture
def sample_match_state():
    """Create a sample MatchState"""
    return MatchState(
        sofa_id=12345,
        betfair_id="31679991",
        search_result={"match_id": "31679991"},
        markets={
            "MATCH_ODDS": "success",
            "OVER_UNDER_25": "failed",
            "CORRECT_SCORE": "not_attempted",
        },
        download_attempts=1,
        archive_path="data/archive/test_run/31679991",
    )


class TestMatchState:
    """Tests for MatchState dataclass"""

    def test_match_state_initialization(self):
        """Test MatchState can be initialized with default values"""
        state = MatchState(sofa_id=12345)

        assert state.sofa_id == 12345
        assert state.betfair_id is None
        assert state.search_result is None
        assert state.markets == {}
        assert state.download_attempts == 0
        assert state.archive_path is None
        assert state.last_updated is not None
        assert state.search_cached is False
        assert state.search_error is None

    def test_match_state_with_values(self, sample_search_result):
        """Test MatchState initialization with all values"""
        state = MatchState(
            sofa_id=12345,
            betfair_id="31679991",
            search_result=sample_search_result,
            markets={"MATCH_ODDS": "success"},
            download_attempts=2,
            archive_path="data/archive/run1/31679991",
            search_cached=True,
        )

        assert state.betfair_id == "31679991"
        assert state.search_result == sample_search_result
        assert state.markets["MATCH_ODDS"] == "success"
        assert state.download_attempts == 2
        assert state.search_cached is True

    def test_get_failed_markets(self, sample_match_state):
        """Test getting list of failed markets"""
        failed = sample_match_state.get_failed_markets()

        assert len(failed) == 1
        assert "OVER_UNDER_25" in failed
        assert "MATCH_ODDS" not in failed
        assert "CORRECT_SCORE" not in failed

    def test_get_not_attempted_markets(self, sample_match_state):
        """Test getting list of not attempted markets"""
        not_attempted = sample_match_state.get_not_attempted_markets()

        assert len(not_attempted) == 1
        assert "CORRECT_SCORE" in not_attempted

    def test_has_failures(self, sample_match_state):
        """Test checking if match has any failures"""
        assert sample_match_state.has_failures() is True

        # Test with no failures
        state_no_failures = MatchState(
            sofa_id=12345, markets={"MATCH_ODDS": "success", "OVER_UNDER_25": "success"}
        )
        assert state_no_failures.has_failures() is False

    def test_is_fully_successful(self, sample_match_state):
        """Test checking if all markets are successful"""
        assert sample_match_state.is_fully_successful() is False

        # Test with all successful
        state_successful = MatchState(
            sofa_id=12345,
            betfair_id="31679991",
            markets={"MATCH_ODDS": "success", "OVER_UNDER_25": "success"},
        )
        assert state_successful.is_fully_successful() is True

        # Test with not_attempted markets
        state_partial = MatchState(
            sofa_id=12345,
            markets={"MATCH_ODDS": "success", "OVER_UNDER_25": "not_attempted"},
        )
        assert state_partial.is_fully_successful() is False

    def test_is_searchable(self):
        """Test checking if match can be searched"""
        # Searchable - no search done yet
        state1 = MatchState(sofa_id=12345)
        assert state1.is_searchable() is True

        # Not searchable - has betfair_id
        state2 = MatchState(sofa_id=12345, betfair_id="31679991")
        assert state2.is_searchable() is False

        # Searchable - has search error
        state3 = MatchState(sofa_id=12345, search_error="Not found")
        assert state3.is_searchable() is True

    def test_needs_retry(self):
        """Test checking if match needs retry"""
        # Needs retry - has failures and attempts < max
        state1 = MatchState(
            sofa_id=12345, markets={"MATCH_ODDS": "failed"}, download_attempts=1
        )
        assert state1.needs_retry(max_attempts=3) is True

        # No retry - max attempts reached
        state2 = MatchState(
            sofa_id=12345, markets={"MATCH_ODDS": "failed"}, download_attempts=3
        )
        assert state2.needs_retry(max_attempts=3) is False

        # No retry - no failures
        state3 = MatchState(
            sofa_id=12345, markets={"MATCH_ODDS": "success"}, download_attempts=0
        )
        assert state3.needs_retry(max_attempts=3) is False

    def test_to_dict_from_dict(self, sample_match_state):
        """Test serialization and deserialization"""
        # Convert to dict
        state_dict = sample_match_state.to_dict()

        assert state_dict["sofa_id"] == 12345
        assert state_dict["betfair_id"] == "31679991"
        assert "markets" in state_dict
        assert "last_updated" in state_dict

        # Convert back from dict
        restored_state = MatchState.from_dict(state_dict)

        assert restored_state.sofa_id == sample_match_state.sofa_id
        assert restored_state.betfair_id == sample_match_state.betfair_id
        assert restored_state.markets == sample_match_state.markets
        assert restored_state.download_attempts == sample_match_state.download_attempts


class TestProcessingStateManager:
    """Tests for ProcessingStateManager"""

    def test_initialization(self, temp_state_dir):
        """Test ProcessingStateManager initialization"""
        manager = ProcessingStateManager(output_dir=temp_state_dir, run_name="test_run")

        assert manager.run_name == "test_run"
        assert manager.max_download_attempts == 3
        assert manager.state_file == temp_state_dir / "test_run_state.json"
        assert len(manager.processing_state) == 0
        assert manager.metadata["run_name"] == "test_run"
        assert manager.metadata["version"] == StateVersion.V1.value

    def test_update_match_search_success(self, temp_state_dir, sample_search_result):
        """Test updating match with successful search"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        manager.update_match_search(
            sofa_id=12345, betfair_id="31679991", search_result=sample_search_result
        )

        assert 12345 in manager.processing_state
        state = manager.processing_state[12345]
        assert state.betfair_id == "31679991"
        assert state.search_result == sample_search_result
        assert state.search_cached is True
        assert state.search_error is None

    def test_update_match_search_failure(self, temp_state_dir):
        """Test updating match with failed search"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        manager.update_match_search(sofa_id=12345, search_error="Match not found")

        assert 12345 in manager.processing_state
        state = manager.processing_state[12345]
        assert state.betfair_id is None
        assert state.search_error == "Match not found"
        assert state.search_cached is False

    def test_update_match_downloads(self, temp_state_dir):
        """Test updating match download status"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # First create a match
        manager.update_match_search(12345, betfair_id="31679991")

        # Update download status
        market_results = {
            "MATCH_ODDS": "success",
            "OVER_UNDER_25": "failed",
            "CORRECT_SCORE": "success",
        }

        manager.update_match_downloads(12345, market_results)

        state = manager.processing_state[12345]
        assert state.markets == market_results
        assert state.download_attempts == 1

    def test_increment_download_attempts(self, temp_state_dir):
        """Test incrementing download attempts"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        manager.update_match_search(12345, betfair_id="31679991")

        # First update
        manager.update_match_downloads(12345, {"MATCH_ODDS": "failed"})
        assert manager.processing_state[12345].download_attempts == 1

        # Second update
        manager.update_match_downloads(12345, {"MATCH_ODDS": "failed"})
        assert manager.processing_state[12345].download_attempts == 2

    def test_update_archive_path(self, temp_state_dir):
        """Test updating archive path"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_archive_path(12345, "data/archive/test_run/31679991")

        state = manager.processing_state[12345]
        assert state.archive_path == "data/archive/test_run/31679991"

    def test_get_match_state(self, temp_state_dir):
        """Test getting match state"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Non-existent match
        assert manager.get_match_state(12345) is None

        # Existing match
        manager.update_match_search(12345, betfair_id="31679991")
        state = manager.get_match_state(12345)
        assert state is not None
        assert state.betfair_id == "31679991"

    def test_get_matches_with_failures(self, temp_state_dir):
        """Test getting matches that have failures"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Add matches with different statuses
        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_match_downloads(
            12345, {"MATCH_ODDS": "success", "OVER_UNDER_25": "failed"}
        )

        manager.update_match_search(12346, betfair_id="31679992")
        manager.update_match_downloads(
            12346, {"MATCH_ODDS": "success", "OVER_UNDER_25": "success"}
        )

        manager.update_match_search(12347, betfair_id="31679993")
        manager.update_match_downloads(12347, {"MATCH_ODDS": "failed"})

        failures = manager.get_matches_with_failures()

        assert len(failures) == 2
        sofa_ids = [sofa_id for sofa_id, _ in failures]
        assert 12345 in sofa_ids
        assert 12347 in sofa_ids
        assert 12346 not in sofa_ids

    def test_get_retry_candidates(self, temp_state_dir):
        """Test getting matches that need retry"""
        manager = ProcessingStateManager(
            temp_state_dir, "test_run", max_download_attempts=3
        )

        # Match that needs retry (attempts < max)
        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_match_downloads(12345, {"MATCH_ODDS": "failed"})

        # Match that doesn't need retry (max attempts reached)
        manager.update_match_search(12346, betfair_id="31679992")
        for _ in range(3):
            manager.update_match_downloads(12346, {"MATCH_ODDS": "failed"})

        # Match without failures
        manager.update_match_search(12347, betfair_id="31679993")
        manager.update_match_downloads(12347, {"MATCH_ODDS": "success"})

        candidates = manager.get_retry_candidates()

        assert len(candidates) == 1
        sofa_id, state = candidates[0]
        assert sofa_id == 12345
        assert state.download_attempts == 1

    def test_get_searchable_matches(self, temp_state_dir):
        """Test getting matches that need search"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Add match without search
        manager.processing_state[12345] = MatchState(sofa_id=12345)

        # Add match with search error
        manager.update_match_search(12346, search_error="Not found")

        # Add match with successful search
        manager.update_match_search(12347, betfair_id="31679993")

        searchable = manager.get_searchable_matches()

        assert len(searchable) == 2
        sofa_ids = [sofa_id for sofa_id, _ in searchable]
        assert 12345 in sofa_ids
        assert 12346 in sofa_ids
        assert 12347 not in sofa_ids

    def test_save_and_load_state(self, temp_state_dir):
        """Test saving and loading state to/from JSON"""
        # Create and populate manager
        manager1 = ProcessingStateManager(temp_state_dir, "test_run")

        manager1.update_match_search(12345, betfair_id="31679991")
        manager1.update_match_downloads(
            12345, {"MATCH_ODDS": "success", "OVER_UNDER_25": "failed"}
        )
        manager1.update_archive_path(12345, "data/archive/test_run/31679991")

        # Save state
        manager1.save_state()
        assert manager1.state_file.exists()

        # Load state in new manager
        manager2 = ProcessingStateManager(temp_state_dir, "test_run")
        manager2.load_state()

        # Verify loaded state
        assert len(manager2.processing_state) == 1
        assert 12345 in manager2.processing_state

        state = manager2.processing_state[12345]
        assert state.betfair_id == "31679991"
        assert state.markets["MATCH_ODDS"] == "success"
        assert state.markets["OVER_UNDER_25"] == "failed"
        assert state.archive_path == "data/archive/test_run/31679991"

    def test_load_nonexistent_state(self, temp_state_dir):
        """Test loading when state file doesn't exist"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Should not raise exception
        manager.load_state()

        assert len(manager.processing_state) == 0

    def test_save_state_with_backup(self, temp_state_dir):
        """Test that saving creates backup if file exists"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # First save
        manager.update_match_search(12345, betfair_id="31679991")
        manager.save_state()

        # Second save should create backup
        manager.update_match_search(12346, betfair_id="31679992")
        manager.save_state()

        # Check backup exists
        backup_files = list(temp_state_dir.glob("test_run_state.json.backup*"))
        assert len(backup_files) > 0

    def test_get_statistics(self, temp_state_dir):
        """Test getting processing statistics"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Add various matches
        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_match_downloads(
            12345, {"MATCH_ODDS": "success", "OVER_UNDER_25": "success"}
        )

        manager.update_match_search(12346, betfair_id="31679992")
        manager.update_match_downloads(12346, {"MATCH_ODDS": "failed"})

        manager.update_match_search(12347, search_error="Not found")

        stats = manager.get_statistics()

        assert stats["total_matches"] == 3
        assert stats["successful_searches"] == 2
        assert stats["failed_searches"] == 1
        assert stats["matches_with_failures"] == 1
        assert stats["fully_successful_matches"] == 1
        assert stats["retry_candidates"] == 1

    def test_clear_state(self, temp_state_dir):
        """Test clearing state"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_match_search(12346, betfair_id="31679992")

        assert len(manager.processing_state) == 2

        manager.clear_state()

        assert len(manager.processing_state) == 0
        assert manager.metadata["run_name"] == "test_run"

    def test_thread_safety_simulation(self, temp_state_dir):
        """Test that state updates are handled correctly"""
        manager = ProcessingStateManager(temp_state_dir, "test_run")

        # Simulate concurrent updates to same match
        manager.update_match_search(12345, betfair_id="31679991")
        manager.update_match_downloads(12345, {"MATCH_ODDS": "success"})
        manager.update_match_downloads(12345, {"OVER_UNDER_25": "failed"})

        state = manager.get_match_state(12345)

        # Both markets should be present
        assert "MATCH_ODDS" in state.markets
        assert "OVER_UNDER_25" in state.markets
        assert state.download_attempts == 2
