import json
import logging
import threading
import time  # Added for rate limit monitor shutdown
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import betfairlightweight
import numpy as np
import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult

# Added for rate limiting
from whatstheodds.betfair.rate_limiter import RateLimitedContext, betfair_rate_limiter
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


@dataclass
class BetfairMatchFiles:
    """Results from search_match_all - contains all market files for a match"""

    sofa_match_id: int
    home: str
    away: str
    date: str
    country: str
    betfair_match_id: Optional[int]
    market_files: List[str]
    expected_count: int

    @classmethod
    def from_search_request(
        cls,
        search_request: BetfairSearchRequest,
        file_list: List[str],
        expected_count: int,
    ) -> "BetfairMatchFiles":
        """
        Create BetfairMatchFiles from search request and file list

        Args:
            search_request: The original search request
            file_list: List of file paths returned from search
            expected_count: Expected number of markets from config
        """
        # Extract betfair_match_id from file path if files exist
        betfair_id = None
        if file_list and len(file_list) > 0:
            # Path format: /xds_nfs/edp_processed/BASIC/2023/Sep/16/32591699/1.217637423.bz2
            #                                                        ^^^^^^^^ match_id
            try:
                path_parts = file_list[0].split("/")
                betfair_id = int(path_parts[-2])
            except (IndexError, ValueError) as e:
                logger.warning(
                    f"Could not extract betfair_id from path: {file_list[0]}"
                )

        # Convert numpy types if needed
        sofa_id = (
            int(search_request.sofa_match_id)
            if hasattr(search_request.sofa_match_id, "item")
            else search_request.sofa_match_id
        )

        return cls(
            sofa_match_id=sofa_id,
            home=search_request.home,
            away=search_request.away,
            date=str(search_request.date),
            country=search_request.country,
            betfair_match_id=betfair_id,
            market_files=file_list,
            expected_count=expected_count,
        )

    @property
    def status(self) -> str:
        """Determine match status based on files found"""
        if not self.market_files:
            return "failed"
        elif len(self.market_files) == self.expected_count:
            return "complete"
        else:
            # Still partial but we'll treat as failed for simplicity
            return "failed"

    @property
    def files_found(self) -> int:
        """Number of market files found"""
        return len(self.market_files)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON storage"""
        return {
            "betfair_match_id": self.betfair_match_id,
            "home": self.home,
            "away": self.away,
            "date": self.date,
            "country": self.country,
            "market_files": self.market_files,
            "files_count": self.files_found,
            "status": self.status,
        }


class BetfairDetailsGrabber:
    """
    Fast version using search_match_all to get all market files in one search.
    Trades market identification for ~12x speed improvement.
    """

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
        match_mapper: Optional[MatchMapper] = None,
        search_engine: Optional[BetfairSearchEngine] = None,
        output_dir: Optional[Path] = None,
        input_dir: Optional[Path] = None,
    ):
        """
        Initialize BetfairDetailsGrabber

        Args:
            api_client: Betfair API client
            cfg: Configuration object
            match_mapper: Match mapper instance
            search_engine: Search engine instance
            output_dir: Directory for output files (default: current directory)
            input_dir: Directory for input files (default: current directory)
        """
        # Default config
        if cfg is None:
            cfg = load_config(config_dir="configs", config_file="history_test.yaml")
        self.cfg = cfg

        # Directory management
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.input_dir = Path(input_dir) if input_dir else Path.cwd()

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize core components
        self.client = setup_betfair_api_client() if api_client is None else api_client

        # Initialize pipeline components
        self.match_mapper = (
            MatchMapper(cfg=self.cfg) if match_mapper is None else match_mapper
        )

        self.search_engine = (
            BetfairSearchEngine(api_client=self.client, cfg=self.cfg)
            if search_engine is None
            else search_engine
        )

        self.max_workers = self.cfg.grabber.max_number_of_workers
        self.results_lock = Lock()
        self._monitoring_active = False  # Added for rate limit monitor

        logger.info("BetfairDetailsGrabber (Fast Version) initialized")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Markets to search: {self.cfg.betfair_football.markets}")
        logger.info(f"Expected market count: {len(self.cfg.betfair_football.markets)}")
        logger.info(f"Threading enabled with {self.max_workers} workers")

    # ========== Helper
    def _process_single_match_threaded(
        self, row: pd.Series
    ) -> Tuple[str, Optional[Dict]]:
        """
        Thread-safe processing of a single match.
        Returns tuple of (sofa_id, result_dict)
        """
        sofa_id = str(row["match_id"])

        try:
            # This calls your existing process_from_row
            match_files = self.process_from_row(row)

            if match_files:
                return (sofa_id, match_files.to_dict())
            else:
                # Create failed result
                return (
                    sofa_id,
                    {
                        "betfair_match_id": None,
                        "home": row.get("home_team"),
                        "away": row.get("away_team"),
                        "date": str(row.get("match_date")),
                        "country": None,
                        "market_files": [],
                        "files_count": 0,
                        "status": "failed",
                    },
                )
        except Exception as e:
            logger.error(f"Thread error processing {sofa_id}: {e}")
            return (
                sofa_id,
                {
                    "betfair_match_id": None,
                    "home": row.get("home_team"),
                    "away": row.get("away_team"),
                    "date": str(row.get("match_date")),
                    "country": None,
                    "market_files": [],
                    "files_count": 0,
                    "status": "failed",
                    "error": str(e),
                },
            )

    # ========== Core Processing Methods ==========

    def map_match_from_row(self, row: pd.Series) -> Optional[BetfairSearchRequest]:
        """Map a row from a df to a Search Request"""
        try:
            return self.match_mapper.map_match_from_row(row=row)
        except Exception as e:
            logger.error(f"Error in match mapping: {str(e)}")
            return None

    def search_match_all(
        self, search_request: BetfairSearchRequest
    ) -> Optional[BetfairMatchFiles]:
        """
        Fast search for match - returns all market files at once

        Args:
            search_request: Search request with match details

        Returns:
            BetfairMatchFiles with all market files or None if failed
        """
        try:
            # Use the fast search method that returns all files
            # Wrap API call with RateLimitedContext
            with RateLimitedContext():
                file_list = self.search_engine.search_match(search_request)

            if file_list is None:
                logger.warning(
                    f"No files found for {search_request.home} vs {search_request.away}"
                )
                return None

            # Create BetfairMatchFiles from results
            match_files = BetfairMatchFiles.from_search_request(
                search_request=search_request,
                file_list=file_list if file_list else [],
                expected_count=len(self.cfg.betfair_football.markets),
            )

            logger.debug(
                f"Found {match_files.files_found}/{match_files.expected_count} files for match {match_files.sofa_match_id}"
            )

            return match_files

        except Exception as e:
            logger.error(f"Error in match search: {str(e)}")
            return None

    def process_from_row(self, row: pd.Series) -> Optional[BetfairMatchFiles]:
        """Process a single row to get BetfairMatchFiles"""
        search_request = self.map_match_from_row(row)

        if search_request is None:
            logger.warning(
                f"Failed to get search_request for match_id: {row.get('match_id')}"
            )
            return None

        match_files = self.search_match_all(search_request=search_request)

        if match_files is None:
            logger.warning(
                f"Failed to get search results for match_id: {row.get('match_id')}"
            )
            return None

        return match_files

    # HACK: - add to configs
    def process_batch_threaded(
        self,
        matches_df: pd.DataFrame,
        output_filename: str = "betfair_results.json",
        mode: str = "new_only",
        force_rerun: bool = False,
        checkpoint_interval: int = 100,
    ) -> Dict:
        """
        Process matches using thread pool for faster execution.
        All parameters same as process_batch.
        """
        output_path = self.output_dir / output_filename

        # Load existing results
        existing = self.load_results(output_path) if output_path.exists() else None

        # Initialize results structure (same as before)
        if existing:
            results = existing
        else:
            results = {
                "metadata": {
                    "markets_searched": list(self.cfg.betfair_football.markets),
                    "expected_markets_count": len(self.cfg.betfair_football.markets),
                    "last_updated": None,
                    "total_processed": 0,
                },
                "matches": {},
            }

        # Identify work needed
        work_dict = self.identify_work_needed(matches_df, existing)

        # Select matches based on mode (same logic as before)
        to_process = []
        if mode == "new_only":
            to_process = work_dict["new"]
        elif mode == "new_and_failed":
            to_process = work_dict["new"] + work_dict["failed"]
        elif mode == "all" and force_rerun:
            to_process = matches_df["match_id"].astype(str).tolist()
        else:
            to_process = work_dict["new"]

        # Log processing plan
        logger.info(f"=" * 60)
        logger.info(f"THREADED PROCESSING - {self.max_workers} workers")
        logger.info(f"Processing mode: {mode}")
        logger.info(f"Total matches in DataFrame: {len(matches_df)}")
        logger.info(f"Matches to process: {len(to_process)}")
        logger.info(f"Already complete: {len(work_dict['complete'])}")
        logger.info(f"New matches: {len(work_dict['new'])}")
        logger.info(f"Failed matches: {len(work_dict['failed'])}")
        logger.info(
            f"Expected markets per match: {results['metadata']['expected_markets_count']}"
        )
        logger.info(f"=" * 60)

        if not to_process:
            logger.info("No matches to process")
            return results

        # Filter DataFrame to matches we need to process
        matches_to_process = matches_df[
            matches_df["match_id"].astype(str).isin(to_process)
        ]

        # Initialize rate limit monitor
        rate_monitor = self._create_rate_limit_monitor(position=1)
        pbar_position = 0 if rate_monitor else None

        # Process with thread pool
        complete_count = 0
        failed_count = 0
        processed_count = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = []
            for _, row in matches_to_process.iterrows():
                future = executor.submit(self._process_single_match_threaded, row)
                futures.append(future)

            # Process completed futures with progress bar
            with tqdm(
                total=len(futures), desc="Processing matches", position=pbar_position
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        sofa_id, result_dict = future.result(timeout=30)

                        # Thread-safe update of results
                        with self.results_lock:
                            results["matches"][sofa_id] = result_dict

                            if result_dict.get("status") == "complete":
                                complete_count += 1
                            else:
                                failed_count += 1

                            processed_count += 1

                            # Checkpoint save
                            if processed_count % checkpoint_interval == 0:
                                results["metadata"][
                                    "last_updated"
                                ] = datetime.now().isoformat()
                                results["metadata"]["total_processed"] = len(
                                    results["matches"]
                                )
                                self.save_results(results, output_path)
                                logger.info(
                                    f"Checkpoint: {processed_count} processed "
                                    f"({complete_count} complete, {failed_count} failed)"
                                )

                    except Exception as e:
                        logger.error(f"Future failed: {e}")
                        failed_count += 1

                    # Update progress bar with current stats
                    pbar.update(1)
                    pbar.set_postfix(
                        {
                            "complete": complete_count,
                            "failed": failed_count,
                            "rate": f"{processed_count/(pbar.format_dict['elapsed'] or 1):.1f}/s",
                        }
                    )

        # Stop rate limit monitor
        if rate_monitor:
            self._monitoring_active = False
            time.sleep(0.6)  # Allow thread to close cleanly

        # Final save
        results["metadata"]["last_updated"] = datetime.now().isoformat()
        results["metadata"]["total_processed"] = len(results["matches"])
        self.save_results(results, output_path)

        # Print summary
        self.print_processing_summary(results)

        # Log rate limiter final status
        rate_status = (
            self.search_engine.client.historic.rate_limiter.get_current_usage()
            if hasattr(self.search_engine.client.historic, "rate_limiter")
            else None
        )
        if rate_status:
            logger.info(f"Final rate limiter status: {rate_status}")

        return results

    # ========== Resume/Rerun Logic ==========

    def identify_work_needed(
        self, matches_df: pd.DataFrame, existing_results: Optional[Dict] = None
    ) -> Dict[str, List[str]]:
        """
        Identify what needs to be processed (simplified: new, failed, complete only)

        Returns:
            Dict with keys: new, failed, complete
        """
        if not existing_results or "matches" not in existing_results:
            return {
                "new": matches_df["match_id"].astype(str).tolist(),
                "failed": [],
                "complete": [],
            }

        work: Dict = {"new": [], "failed": [], "complete": []}
        expected_count = existing_results["metadata"]["expected_markets_count"]

        for _, row in matches_df.iterrows():
            sofa_id = str(row["match_id"])

            if sofa_id not in existing_results["matches"]:
                work["new"].append(sofa_id)
            else:
                match_data = existing_results["matches"][sofa_id]
                files_count = match_data.get("files_count", 0)
                status = match_data.get("status", "failed")

                if status == "complete" and files_count == expected_count:
                    work["complete"].append(sofa_id)
                else:
                    work["failed"].append(sofa_id)

        return work

    # ========== Main Processing Method ==========
    def process_batch(
        self,
        matches_df: pd.DataFrame,
        output_filename: str = "betfair_results.json",
        mode: str = "new_only",
        force_rerun: bool = False,
        use_threading: bool = True,  # ADD THIS PARAMETER
    ) -> Dict:
        """
        Process matches - now with threading support by default.

        Args:
            ... existing args ...
            use_threading: If True, use threaded processing (default: True)

        Process matches with different modes (simplified)

        Args:
            matches_df: DataFrame with matches to process
            output_filename: Name of output JSON file
            mode: Processing mode
                - "new_only": Only process new matches (default)
                - "new_and_failed": Process new + failed matches
                - "all": Reprocess everything (if force_rerun=True)
            force_rerun: Allow reprocessing of all matches
            checkpoint_interval: Save progress every N matches

        Returns:
            Dict with results

        """
        if use_threading:
            return self.process_batch_threaded(
                matches_df=matches_df,
                output_filename=output_filename,
                mode=mode,
                force_rerun=force_rerun,
                checkpoint_interval=self.cfg.grabber.checkpoint_interval,
            )
        else:
            # Fall back to sequential processing
            return self.process_batch_sequential(
                matches_df=matches_df,
                output_filename=output_filename,
                mode=mode,
                force_rerun=force_rerun,
                checkpoint_interval=self.cfg.grabber.checkpoint_interval,
            )

    def process_batch_sequential(
        self,
        matches_df: pd.DataFrame,
        output_filename: str = "betfair_results.json",
        mode: str = "new_only",
        force_rerun: bool = False,
        checkpoint_interval: int = 100,
    ) -> Dict:
        """
        Process matches with different modes (simplified)

        Args:
            matches_df: DataFrame with matches to process
            output_filename: Name of output JSON file
            mode: Processing mode
                - "new_only": Only process new matches (default)
                - "new_and_failed": Process new + failed matches
                - "all": Reprocess everything (if force_rerun=True)
            force_rerun: Allow reprocessing of all matches
            checkpoint_interval: Save progress every N matches

        Returns:
            Dict with results
        """
        output_path = self.output_dir / output_filename

        # Load existing results
        existing = self.load_results(output_path) if output_path.exists() else None

        # Initialize results structure
        if existing:
            results = existing
        else:
            results = {
                "metadata": {
                    "markets_searched": list(self.cfg.betfair_football.markets),
                    "expected_markets_count": len(self.cfg.betfair_football.markets),
                    "last_updated": None,
                    "total_processed": 0,
                },
                "matches": {},
            }

        # Identify work needed
        work_dict = self.identify_work_needed(matches_df, existing)

        # Select matches based on mode
        to_process = []
        if mode == "new_only":
            to_process = work_dict["new"]
        elif mode == "new_and_failed":
            to_process = work_dict["new"] + work_dict["failed"]
        elif mode == "all" and force_rerun:
            to_process = matches_df["match_id"].astype(str).tolist()
        else:
            logger.warning(f"Invalid mode: {mode}")
            to_process = work_dict["new"]

        # Log processing plan
        logger.info(f"=" * 60)
        logger.info(f"Processing mode: {mode}")
        logger.info(f"Total matches in DataFrame: {len(matches_df)}")
        logger.info(f"Matches to process: {len(to_process)}")
        logger.info(f"Already complete: {len(work_dict['complete'])}")
        logger.info(f"New matches: {len(work_dict['new'])}")
        logger.info(f"Failed matches: {len(work_dict['failed'])}")
        logger.info(
            f"Expected markets per match: {results['metadata']['expected_markets_count']}"
        )
        logger.info(f"=" * 60)

        if not to_process:
            logger.info("No matches to process")
            return results

        # Filter DataFrame to matches we need to process
        matches_to_process = matches_df[
            matches_df["match_id"].astype(str).isin(to_process)
        ]

        # Process with checkpoints
        processed_count = 0
        complete_count = 0
        failed_count = 0

        for idx, (_, row) in enumerate(
            tqdm(
                matches_to_process.iterrows(),
                total=len(matches_to_process),
                desc="Processing matches",
            )
        ):
            sofa_id = str(row["match_id"])

            try:
                # Process the match
                match_files = self.process_from_row(row)

                if match_files:
                    # Store result
                    results["matches"][sofa_id] = match_files.to_dict()

                    if match_files.status == "complete":
                        complete_count += 1
                    else:
                        failed_count += 1
                else:
                    # Store error if search failed
                    results["matches"][sofa_id] = {
                        "betfair_match_id": None,
                        "home": row.get("home_team"),
                        "away": row.get("away_team"),
                        "date": str(row.get("match_date")),
                        "country": None,
                        "market_files": [],
                        "files_count": 0,
                        "status": "failed",
                    }
                    failed_count += 1

                processed_count += 1

                # Checkpoint save
                if processed_count % checkpoint_interval == 0:
                    results["metadata"]["last_updated"] = datetime.now().isoformat()
                    results["metadata"]["total_processed"] = len(results["matches"])
                    self.save_results(results, output_path)
                    logger.info(
                        f"Checkpoint: {processed_count} processed ({complete_count} complete, {failed_count} failed)"
                    )

            except Exception as e:
                logger.error(f"Error processing match {sofa_id}: {e}")
                results["matches"][sofa_id] = {
                    "betfair_match_id": None,
                    "home": row.get("home_team"),
                    "away": row.get("away_team"),
                    "date": str(row.get("match_date")),
                    "country": None,
                    "market_files": [],
                    "files_count": 0,
                    "status": "failed",
                }
                failed_count += 1

        # Final save
        results["metadata"]["last_updated"] = datetime.now().isoformat()
        results["metadata"]["total_processed"] = len(results["matches"])
        self.save_results(results, output_path)

        # Print summary
        self.print_processing_summary(results)

        return results

    # ========== Helper Methods ==========

    def _create_rate_limit_monitor(self, position: int = 1) -> Optional[tqdm]:
        """Creates a separate progress bar to monitor API rate limit usage."""
        rate_bar = tqdm(
            total=100,
            desc="Rate Limit",
            position=position,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {postfix}",
            leave=False,
            colour="yellow",
        )
        self._monitoring_active = True

        def update_rate_bar():
            """Background task to update the rate limit bar with current status."""
            while self._monitoring_active:
                try:
                    status = betfair_rate_limiter.get_current_usage()

                    rate_bar.n = int(status["window_usage_percent"])
                    rate_bar.colour = (
                        "red"
                        if status["window_usage_percent"] > 90
                        else (
                            "yellow" if status["window_usage_percent"] > 70 else "green"
                        )
                    )
                    rate_bar.set_postfix(
                        {
                            "reqs": f"{status['current_requests_in_window']}/{status['max_requests_in_window']}",
                            "concurrent": f"{status['active_concurrent_downloads']}/{status['max_concurrent_downloads']}",
                        }
                    )
                    rate_bar.refresh()
                except Exception:
                    # Fail silently to not interrupt the main download
                    pass

                time.sleep(0.5)

            rate_bar.close()

        monitor_thread = threading.Thread(target=update_rate_bar, daemon=True)
        monitor_thread.start()
        return rate_bar

    def _is_complete(self, sofa_id: str, results: Dict) -> bool:
        """Check if a match is complete (all expected markets found)"""
        if sofa_id not in results["matches"]:
            return False

        match = results["matches"][sofa_id]
        expected_count = results["metadata"]["expected_markets_count"]

        return (
            match.get("status") == "complete"
            and match.get("files_count", 0) == expected_count
        )

    def get_threading_stats(self) -> Dict:
        """Get current threading and rate limiting statistics"""
        stats = {
            "max_workers": self.max_workers,
            "threading_enabled": True,
        }

        # Try to get rate limiter status
        try:
            from whatstheodds.betfair.rate_limiter import betfair_rate_limiter

            rate_status = betfair_rate_limiter.get_current_usage()
            stats["rate_limiter"] = rate_status
        except:
            pass

        return stats

    # ========== I/O Methods ==========

    def _json_encoder(self, obj):
        """Custom JSON encoder for numpy types"""
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        else:
            return str(obj)

    def save_results(self, results: Dict, output_path: Path) -> None:
        """Save results to JSON file"""
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=self._json_encoder)
        logger.debug(f"Results saved to {output_path}")

    def load_results(self, input_path: Path) -> Optional[Dict]:
        """Load results from JSON file"""
        try:
            with open(input_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading results from {input_path}: {e}")
            return None

    def load_matches_dataframe(self, filename: str) -> pd.DataFrame:
        """Load matches DataFrame from input directory"""
        file_path = self.input_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")

        logger.info(f"Loading matches from: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} matches")
        return df

    # ========== Summary Methods ==========

    def get_market_coverage_summary(
        self, results_path: Optional[Path] = None, results: Optional[Dict] = None
    ) -> Dict:
        """Calculate market coverage statistics (simplified: complete or failed only)"""
        if not results:
            if not results_path:
                results_path = self.output_dir / "betfair_results.json"
            results = self.load_results(results_path)

        if not results:
            return {}

        expected_count = results["metadata"]["expected_markets_count"]

        complete_matches = 0
        failed_matches = 0

        for match_data in results["matches"].values():
            if (
                match_data.get("status") == "complete"
                and match_data.get("files_count") == expected_count
            ):
                complete_matches += 1
            else:
                failed_matches += 1

        total = len(results["matches"])
        completeness_rate = (complete_matches / total * 100) if total > 0 else 0

        return {
            "match_summary": {
                "total": total,
                "complete": complete_matches,
                "failed": failed_matches,
                "completeness_rate": completeness_rate,
            },
            "expected_markets_count": expected_count,
            "markets_searched": results["metadata"].get("markets_searched", []),
        }

    def get_failed_matches(self, results_path: Optional[Path] = None) -> pd.DataFrame:
        """Get DataFrame of failed matches for investigation"""
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        results = self.load_results(results_path)
        if not results:
            return pd.DataFrame()

        expected_count = results["metadata"]["expected_markets_count"]

        failed = []
        for sofa_id, match_data in results["matches"].items():
            if (
                match_data.get("status") != "complete"
                or match_data.get("files_count", 0) != expected_count
            ):
                failed.append(
                    {
                        "sofa_id": sofa_id,
                        "betfair_id": match_data.get("betfair_match_id"),
                        "home": match_data.get("home"),
                        "away": match_data.get("away"),
                        "date": match_data.get("date"),
                        "files_found": match_data.get("files_count", 0),
                        "files_expected": expected_count,
                        "status": match_data.get("status", "failed"),
                    }
                )

        return pd.DataFrame(failed)

    def get_complete_matches(self, results_path: Optional[Path] = None) -> Dict:
        """
        Get matches ready for download (complete matches only)

        Args:
            results_path: Path to results JSON

        Returns:
            Dict of complete matches
        """
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        results = self.load_results(results_path)
        if not results:
            return {}

        expected_count = results["metadata"]["expected_markets_count"]

        complete = {}
        for sofa_id, match_data in results["matches"].items():
            if (
                match_data.get("status") == "complete"
                and match_data.get("files_count") == expected_count
            ):
                complete[sofa_id] = match_data

        return complete

    def print_processing_summary(self, results: Dict) -> None:
        """Print a summary of processing results"""
        summary = self.get_market_coverage_summary(results=results)

        if not summary:
            return

        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY (Fast Mode)")
        print("=" * 60)

        # Match summary
        match_summary = summary["match_summary"]
        print(f"\nMatch Statistics:")
        print(f"  Total processed: {match_summary['total']}")
        print(
            f"  Complete (all {summary['expected_markets_count']} markets): {match_summary['complete']}"
        )
        print(f"  Failed/Incomplete: {match_summary['failed']}")
        print(f"  Success rate: {match_summary['completeness_rate']:.1f}%")

        # Markets searched
        markets_list = summary.get("markets_searched", [])
        if markets_list:
            print(f"\nMarkets searched ({len(markets_list)}):")
            for i in range(0, len(markets_list), 4):
                batch = markets_list[i : i + 4]
                print(f"  {', '.join(batch)}")

        # Visual bar
        rate = match_summary["completeness_rate"]
        bar_length = 40
        filled_length = int(bar_length * rate / 100)
        bar = "█" * filled_length + "░" * (bar_length - filled_length)
        print(f"\nProgress: {bar} {rate:.1f}%")

        print("=" * 60 + "\n")

    def export_analysis_reports(self, results_path: Optional[Path] = None) -> None:
        """Export various analysis reports"""
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        # Export failed matches
        failed_df = self.get_failed_matches(results_path)
        if not failed_df.empty:
            failed_path = self.output_dir / "failed_matches.csv"
            failed_df.to_csv(failed_path, index=False)
            logger.info(f"Exported {len(failed_df)} failed matches to {failed_path}")

        # Export summary
        summary = self.get_market_coverage_summary(results_path=results_path)
        if summary:
            summary_path = self.output_dir / "market_coverage_summary.json"
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Exported market coverage summary to {summary_path}")

        # Export complete matches
        complete = self.get_complete_matches(results_path)
        if complete:
            download_path = self.output_dir / "ready_for_download.json"
            with open(download_path, "w") as f:
                json.dump(complete, f, indent=2, default=self._json_encoder)
            logger.info(f"Exported {len(complete)} complete matches to {download_path}")

        # Create a simple stats file
        stats_path = self.output_dir / "processing_stats.txt"
        with open(stats_path, "w") as f:
            f.write("BETFAIR PROCESSING STATISTICS\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Results file: {results_path.name}\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            if summary:
                ms = summary["match_summary"]
                f.write(f"Total matches: {ms['total']}\n")
                f.write(f"Complete: {ms['complete']}\n")
                f.write(f"Failed: {ms['failed']}\n")
                f.write(f"Success rate: {ms['completeness_rate']:.1f}%\n\n")

                f.write(
                    f"Expected markets per match: {summary['expected_markets_count']}\n"
                )
                f.write(f"Markets searched:\n")
                for market in summary.get("markets_searched", []):
                    f.write(f"  - {market}\n")

        logger.info(f"Exported processing statistics to {stats_path}")
