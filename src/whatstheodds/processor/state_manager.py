"""
State management for processing pipeline
Tracks match processing state, search results, download status, and retry attempts
"""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class StateVersion(Enum):
    """Version of state file format"""

    V1 = "1.0.0"


@dataclass
class MatchState:
    """State tracking for a single match"""

    sofa_id: int
    betfair_id: Optional[str] = None
    search_result: Optional[Dict[str, Any]] = None
    markets: Dict[str, str] = field(
        default_factory=dict
    )  # market -> "success"/"failed"/"not_attempted"
    download_attempts: int = 0
    archive_path: Optional[str] = None
    last_updated: Optional[str] = field(
        default_factory=lambda: datetime.now().isoformat()
    )
    search_cached: bool = False
    search_error: Optional[str] = None

    def get_failed_markets(self) -> List[str]:
        """Get list of markets that failed to download"""
        return [market for market, status in self.markets.items() if status == "failed"]

    def get_not_attempted_markets(self) -> List[str]:
        """Get list of markets not yet attempted"""
        return [
            market
            for market, status in self.markets.items()
            if status == "not_attempted"
        ]

    def has_failures(self) -> bool:
        """Check if any markets have failed"""
        return any(status == "failed" for status in self.markets.values())

    def is_fully_successful(self) -> bool:
        """Check if all markets were successfully downloaded"""
        if not self.markets:
            return False
        return all(status == "success" for status in self.markets.values())

    def is_searchable(self) -> bool:
        """Check if match needs to be searched (no betfair_id or has search error)"""
        return self.betfair_id is None or self.search_error is not None

    def needs_retry(self, max_attempts: int = 3) -> bool:
        """Check if match needs retry based on failures and attempt count"""
        return self.has_failures() and self.download_attempts < max_attempts

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "sofa_id": int(self.sofa_id),
            "betfair_id": self.betfair_id,
            "search_result": self.search_result,
            "markets": self.markets,
            "download_attempts": int(self.download_attempts),
            "archive_path": self.archive_path,
            "last_updated": self.last_updated,
            "search_cached": bool(self.search_cached),
            "search_error": self.search_error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MatchState":
        """Create MatchState from dictionary"""
        return cls(
            sofa_id=data["sofa_id"],
            betfair_id=data.get("betfair_id"),
            search_result=data.get("search_result"),
            markets=data.get("markets", {}),
            download_attempts=data.get("download_attempts", 0),
            archive_path=data.get("archive_path"),
            last_updated=data.get("last_updated"),
            search_cached=data.get("search_cached", False),
            search_error=data.get("search_error"),
        )


##############################
#
##############################


class ProcessingStateManager:
    """Manages processing state for a run"""

    def __init__(self, output_dir: Path, run_name: str, max_download_attempts: int = 3):
        """
        Initialize state manager

        Args:
            output_dir: Directory to save state files
            run_name: Name of the processing run
            max_download_attempts: Maximum retry attempts for downloads
        """
        self.output_dir = Path(output_dir)
        self.run_name = run_name
        self.max_download_attempts = max_download_attempts

        # State file path
        self.state_file = self.output_dir / f"{run_name}_state.json"

        # Initialize state containers
        self.processing_state: Dict[int, MatchState] = {}
        self.metadata = {
            "run_name": run_name,
            "version": StateVersion.V1.value,
            "created_at": datetime.now().isoformat(),
            "last_saved": None,
            "max_download_attempts": max_download_attempts,
        }

        logger.info(f"Initialized ProcessingStateManager for run '{run_name}'")

    def update_match_search(
        self,
        sofa_id: int,
        betfair_id: Optional[str] = None,
        search_result: Optional[Dict[str, Any]] = None,
        search_error: Optional[str] = None,
    ) -> None:
        """
        Update match search results

        Args:
            sofa_id: Sofa match ID
            betfair_id: Betfair match ID if found
            search_result: Full search result to cache
            search_error: Error message if search failed
        """
        if sofa_id not in self.processing_state:
            self.processing_state[sofa_id] = MatchState(sofa_id=sofa_id)

        state = self.processing_state[sofa_id]

        if betfair_id:
            state.betfair_id = betfair_id
            state.search_cached = True
            state.search_error = None
            logger.debug(f"Updated match {sofa_id} with Betfair ID {betfair_id}")

        if search_result:
            state.search_result = search_result
            state.search_cached = True

        if search_error:
            state.search_error = search_error
            state.search_cached = False
            logger.debug(f"Match {sofa_id} search failed: {search_error}")

        state.last_updated = datetime.now().isoformat()

    def update_match_downloads(
        self, sofa_id: int, betfair_id: int, market_results: Dict[str, str]
    ) -> None:
        """
        Update market download status

        Args:
            sofa_id: Sofa match ID
            market_results: Dict of market_type -> status ("success"/"failed"/"not_attempted")
        """
        # TODO: - add search results into MatchState, Note need to be json friendly

        if sofa_id not in self.processing_state:
            self.processing_state[sofa_id] = MatchState(sofa_id=sofa_id)

        state = self.processing_state[sofa_id]

        # Update market statuses
        state.markets.update(market_results)

        # Increment attempts if there are any updates
        if market_results:
            state.download_attempts += 1

        state.last_updated = datetime.now().isoformat()

        logger.debug(
            f"Updated match {sofa_id} downloads: "
            f"{sum(1 for s in market_results.values() if s == 'success')} success, "
            f"{sum(1 for s in market_results.values() if s == 'failed')} failed"
        )

    def update_archive_path(self, sofa_id: int, archive_path: str) -> None:
        """
        Update archive path for a match

        Args:
            sofa_id: Sofa match ID
            archive_path: Path where match files are archived
        """
        if sofa_id not in self.processing_state:
            self.processing_state[sofa_id] = MatchState(sofa_id=sofa_id)

        state = self.processing_state[sofa_id]
        state.archive_path = archive_path
        state.last_updated = datetime.now().isoformat()

        logger.debug(f"Updated match {sofa_id} archive path: {archive_path}")

    def get_match_state(self, sofa_id: int) -> Optional[MatchState]:
        """Get state for a specific match"""
        return self.processing_state.get(sofa_id)

    def get_matches_with_failures(self) -> List[Tuple[int, MatchState]]:
        """
        Get all matches that have download failures

        Returns:
            List of (sofa_id, MatchState) tuples for matches with failures
        """
        return [
            (sofa_id, state)
            for sofa_id, state in self.processing_state.items()
            if state.has_failures()
        ]

    def get_retry_candidates(self) -> List[Tuple[int, MatchState]]:
        """
        Get matches that should be retried

        Returns:
            List of (sofa_id, MatchState) tuples for matches needing retry
        """
        return [
            (sofa_id, state)
            for sofa_id, state in self.processing_state.items()
            if state.needs_retry(self.max_download_attempts)
        ]

    def get_searchable_matches(self) -> List[Tuple[int, MatchState]]:
        """
        Get matches that need to be searched

        Returns:
            List of (sofa_id, MatchState) tuples for matches needing search
        """
        return [
            (sofa_id, state)
            for sofa_id, state in self.processing_state.items()
            if state.is_searchable()
        ]

    def get_statistics(self) -> Dict[str, Any]:
        """Get summary statistics of processing state"""
        total = len(self.processing_state)

        if total == 0:
            return {
                "total_matches": 0,
                "successful_searches": 0,
                "failed_searches": 0,
                "fully_successful_matches": 0,
                "matches_with_failures": 0,
                "retry_candidates": 0,
                "searchable_matches": 0,
            }

        successful_searches = sum(
            1
            for state in self.processing_state.values()
            if state.betfair_id is not None
        )

        failed_searches = sum(
            1
            for state in self.processing_state.values()
            if state.search_error is not None
        )

        fully_successful = sum(
            1 for state in self.processing_state.values() if state.is_fully_successful()
        )

        with_failures = len(self.get_matches_with_failures())
        retry_candidates = len(self.get_retry_candidates())
        searchable = len(self.get_searchable_matches())

        return {
            "total_matches": total,
            "successful_searches": successful_searches,
            "failed_searches": failed_searches,
            "fully_successful_matches": fully_successful,
            "matches_with_failures": with_failures,
            "retry_candidates": retry_candidates,
            "searchable_matches": searchable,
            "total_download_attempts": sum(
                state.download_attempts for state in self.processing_state.values()
            ),
        }

    def save_state(self) -> None:
        """Save current state to JSON file"""
        # Create backup if file exists
        if self.state_file.exists():
            backup_file = self.state_file.with_suffix(
                f".json.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            self.state_file.rename(backup_file)
            logger.debug(f"Created backup: {backup_file}")

        # Prepare state data
        state_data = {
            "metadata": {**self.metadata, "last_saved": datetime.now().isoformat()},
            "processing_state": {
                str(sofa_id): state.to_dict()
                for sofa_id, state in self.processing_state.items()
            },
        }

        # Save to file
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(state_data, f, indent=2)

        logger.info(
            f"Saved state to {self.state_file} ({len(self.processing_state)} matches)"
        )

    def load_state(self) -> None:
        """Load state from JSON file"""
        if not self.state_file.exists():
            logger.info(f"No existing state file found at {self.state_file}")
            return

        try:
            with open(self.state_file, "r") as f:
                state_data = json.load(f)

            # Load metadata
            self.metadata.update(state_data.get("metadata", {}))

            # Load processing state
            self.processing_state = {}
            for sofa_id_str, match_data in state_data.get(
                "processing_state", {}
            ).items():
                sofa_id = int(sofa_id_str)
                self.processing_state[sofa_id] = MatchState.from_dict(match_data)

            logger.info(
                f"Loaded state from {self.state_file} "
                f"({len(self.processing_state)} matches)"
            )

        except Exception as e:
            logger.error(f"Failed to load state from {self.state_file}: {e}")
            logger.info("Starting with empty state")

    def clear_state(self) -> None:
        """Clear all processing state (keeps metadata)"""
        self.processing_state = {}
        logger.info("Cleared all processing state")

    def export_summary(self) -> Dict[str, Any]:
        """Export a summary of the current state for reporting"""
        stats = self.get_statistics()

        # Get detailed failure information
        failures_by_market: Dict = {}
        for state in self.processing_state.values():
            for market, status in state.markets.items():
                if status == "failed":
                    failures_by_market[market] = failures_by_market.get(market, 0) + 1

        # Get matches at max attempts
        max_attempts_reached = [
            sofa_id
            for sofa_id, state in self.processing_state.items()
            if state.download_attempts >= self.max_download_attempts
            and state.has_failures()
        ]

        return {
            "run_name": self.run_name,
            "statistics": stats,
            "failures_by_market": failures_by_market,
            "max_attempts_reached": max_attempts_reached,
            "metadata": self.metadata,
            "state_file": str(self.state_file),
        }
