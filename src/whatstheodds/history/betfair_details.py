import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import betfairlightweight
import numpy as np
import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


class BetfairDetailsGrabber:
    """
    Coordinates the entire pipeline from match row to extracted betfair match details.
    Provides flexible input/output control and resume capabilities.
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

        logger.info(f"BetfairDetailsGrabber initialized")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Markets to search: {self.cfg.betfair_football.markets}")

    # ========== Core Processing Methods ==========

    def map_match_from_row(self, row: pd.Series) -> Optional[BetfairSearchRequest]:
        """Map a row from a df to a Search Request"""
        try:
            return self.match_mapper.map_match_from_row(row=row)
        except Exception as e:
            logger.error(f"Error in match mapping: {str(e)}")
            return None

    def search_match(
        self, search_request: BetfairSearchRequest
    ) -> Optional[BetfairSearchResult]:
        """Search for match using BetfairSearchEngine"""
        try:
            return self.search_engine.search_main(search_request)
        except Exception as e:
            logger.error(f"Error in match search: {str(e)}")
            return None

    def process_from_row(self, row: pd.Series) -> Optional[BetfairSearchResult]:
        """Process a single row to get BetfairSearchResult"""
        search_request = self.map_match_from_row(row)

        if search_request is None:
            logger.warning(
                f"Failed to get search_request for match_id: {row.get('match_id')}"
            )
            return None

        search_result = self.search_match(search_request=search_request)

        if search_result is None:
            logger.warning(
                f"Failed to get search results for match_id: {row.get('match_id')}"
            )
            return None

        return search_result

    # ========== Resume/Rerun Logic ==========

    def identify_work_needed(
        self, matches_df: pd.DataFrame, existing_results: Optional[Dict] = None
    ) -> Dict[str, List[str]]:
        """
        Identify what needs to be processed

        Returns:
            Dict with keys: new, incomplete, failed, complete
        """
        if not existing_results or "matches" not in existing_results:
            return {
                "new": matches_df["match_id"].astype(str).tolist(),
                "incomplete": [],
                "failed": [],
                "complete": [],
            }

        work: Dict = {"new": [], "incomplete": [], "failed": [], "complete": []}
        expected_markets = set(self.cfg.betfair_football.markets)

        for _, row in matches_df.iterrows():
            sofa_id = str(row["match_id"])

            if sofa_id not in existing_results["matches"]:
                work["new"].append(sofa_id)
            else:
                match_data = existing_results["matches"][sofa_id]

                # Check if it was an error
                if "error" in match_data:
                    work["failed"].append(sofa_id)
                # Check market completeness
                elif "markets" in match_data:
                    found_markets = set(match_data["markets"].keys())
                    if found_markets == expected_markets:
                        work["complete"].append(sofa_id)
                    else:
                        work["incomplete"].append(sofa_id)
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
        checkpoint_interval: int = 100,
    ) -> Dict:
        """
        Process matches with different modes

        Args:
            matches_df: DataFrame with matches to process
            output_filename: Name of output JSON file
            mode: Processing mode
                - "new_only": Only process new matches (default)
                - "new_and_incomplete": Process new + incomplete markets
                - "new_and_failed": Process new + previous failures
                - "all_except_complete": Everything except complete matches
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
                    "markets_requested": list(self.cfg.betfair_football.markets),
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
        elif mode == "new_and_incomplete":
            to_process = work_dict["new"] + work_dict["incomplete"]
        elif mode == "new_and_failed":
            to_process = work_dict["new"] + work_dict["failed"]
        elif mode == "all_except_complete":
            to_process = (
                work_dict["new"] + work_dict["incomplete"] + work_dict["failed"]
            )
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
        logger.info(f"Incomplete matches: {len(work_dict['incomplete'])}")
        logger.info(f"Failed matches: {len(work_dict['failed'])}")
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
                search_result = self.process_from_row(row)

                if search_result:
                    # Store result
                    results["matches"][sofa_id] = self._extract_match_data(
                        search_result
                    )
                else:
                    # Store error if search failed
                    results["matches"][sofa_id] = {
                        "betfair_match_id": None,
                        "error": "Search failed",
                        "home": row.get("home_team"),
                        "away": row.get("away_team"),
                        "date": str(row.get("match_date")),
                    }

                processed_count += 1

                # Checkpoint save
                if processed_count % checkpoint_interval == 0:
                    results["metadata"]["last_updated"] = datetime.now().isoformat()
                    results["metadata"]["total_processed"] = len(results["matches"])
                    self.save_results(results, output_path)
                    logger.info(
                        f"Checkpoint saved: {processed_count} matches processed"
                    )

            except Exception as e:
                logger.error(f"Error processing match {sofa_id}: {e}")
                results["matches"][sofa_id] = {
                    "betfair_match_id": None,
                    "error": f"Processing error: {str(e)}",
                    "home": row.get("home_team"),
                    "away": row.get("away_team"),
                    "date": str(row.get("match_date")),
                }

        # Final save
        results["metadata"]["last_updated"] = datetime.now().isoformat()
        results["metadata"]["total_processed"] = len(results["matches"])
        self.save_results(results, output_path)

        # Print summary
        self.print_processing_summary(results)

        return results

    # ========== Helper Methods ==========

    def _is_complete(self, sofa_id: str, results: Dict) -> bool:
        """Check if a match has all requested markets"""
        if sofa_id not in results["matches"]:
            return False

        match = results["matches"][sofa_id]
        if "error" in match:
            return False

        expected = set(results["metadata"]["markets_requested"])
        found = set(match.get("markets", {}).keys())
        return expected == found

    def _extract_match_data(self, search_result: BetfairSearchResult) -> Dict:
        """Convert BetfairSearchResult to storage format"""
        # Convert match_id to int if it's numpy int64
        match_id = search_result.match_id
        if match_id is not None and isinstance(match_id, (np.integer, np.int64)):
            match_id = int(match_id)

        data = {
            "betfair_match_id": match_id,
            "home": search_result.home,
            "away": search_result.away,
            "date": str(search_result.date) if search_result.date else None,
            "country": search_result.country,
        }

        if search_result.match_id:
            # Store just the file paths for valid markets
            data["markets"] = {
                market_type: market_result.file
                for market_type, market_result in search_result.valid_markets.items()
            }

            # Note any missing markets
            if search_result.missing_markets:
                data["missing_markets_info"] = {
                    market_type: market_result.error or "Not found"
                    for market_type, market_result in search_result.missing_markets.items()
                }
        else:
            data["error"] = "Match not found"

        return data

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
        """Calculate market coverage statistics"""
        if not results:
            if not results_path:
                results_path = self.output_dir / "betfair_results.json"
            results = self.load_results(results_path)

        if not results:
            return {}

        markets_requested = results["metadata"]["markets_requested"]
        coverage = {
            market: {"found": 0, "missing": 0, "percentage": 0.0, "total": 0}
            for market in markets_requested
        }

        total_matches = 0
        complete_matches = 0
        partial_matches = 0
        no_data_matches = 0

        for match_data in results["matches"].values():
            if "error" in match_data:
                no_data_matches += 1
                continue

            total_matches += 1

            if "markets" in match_data:
                markets_found = 0
                for market in markets_requested:
                    if market in match_data["markets"]:
                        coverage[market]["found"] += 1
                        markets_found += 1
                    else:
                        coverage[market]["missing"] += 1

                if markets_found == len(markets_requested):
                    complete_matches += 1
                elif markets_found > 0:
                    partial_matches += 1
                else:
                    no_data_matches += 1

        # Calculate percentages
        for market, stats in coverage.items():
            total = stats["found"] + stats["missing"]
            stats["total"] = total
            stats["percentage"] = (stats["found"] / total * 100) if total > 0 else 0

        return {
            "market_coverage": coverage,
            "match_summary": {
                "total": len(results["matches"]),
                "complete": complete_matches,
                "partial": partial_matches,
                "no_data": no_data_matches,
            },
        }

    def get_incomplete_matches(
        self, results_path: Optional[Path] = None
    ) -> pd.DataFrame:
        """Get DataFrame of incomplete matches for investigation"""
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        results = self.load_results(results_path)
        if not results:
            return pd.DataFrame()

        expected_markets = set(results["metadata"]["markets_requested"])

        incomplete = []
        for sofa_id, match_data in results["matches"].items():
            if "error" in match_data:
                incomplete.append(
                    {
                        "sofa_id": sofa_id,
                        "betfair_id": None,
                        "home": match_data.get("home"),
                        "away": match_data.get("away"),
                        "date": match_data.get("date"),
                        "error": match_data["error"],
                        "markets_found": [],
                        "markets_missing": list(expected_markets),
                    }
                )
            elif "markets" in match_data:
                found = set(match_data["markets"].keys())
                missing = expected_markets - found

                if missing:
                    incomplete.append(
                        {
                            "sofa_id": sofa_id,
                            "betfair_id": match_data.get("betfair_match_id"),
                            "home": match_data.get("home"),
                            "away": match_data.get("away"),
                            "date": match_data.get("date"),
                            "error": None,
                            "markets_found": list(found),
                            "markets_missing": list(missing),
                        }
                    )

        return pd.DataFrame(incomplete)

    def get_downloadable_matches(
        self, results_path: Optional[Path] = None, min_markets: Optional[int] = None
    ) -> Dict:
        """
        Get matches ready for odds download

        Args:
            results_path: Path to results JSON
            min_markets: Minimum number of markets required (default: all)

        Returns:
            Dict of matches meeting criteria
        """
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        results = self.load_results(results_path)
        if not results:
            return {}

        expected_markets = results["metadata"]["markets_requested"]
        if min_markets is None:
            min_markets = len(expected_markets)

        downloadable = {}
        for sofa_id, match_data in results["matches"].items():
            if "markets" in match_data:
                if len(match_data["markets"]) >= min_markets:
                    downloadable[sofa_id] = match_data

        return downloadable

    def print_processing_summary(self, results: Dict) -> None:
        """Print a summary of processing results"""
        summary = self.get_market_coverage_summary(results=results)

        if not summary:
            return

        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)

        # Match summary
        match_summary = summary["match_summary"]
        print(f"\nMatch Statistics:")
        print(f"  Total processed: {match_summary['total']}")
        print(f"  Complete (all markets): {match_summary['complete']}")
        print(f"  Partial (some markets): {match_summary['partial']}")
        print(f"  No data found: {match_summary['no_data']}")

        # Market coverage
        print(f"\nMarket Coverage:")
        for market, stats in summary["market_coverage"].items():
            percentage = stats["percentage"]
            bar_length = 20
            filled_length = int(bar_length * percentage / 100)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            print(
                f"  {market:20} {stats['found']:4}/{stats['total']:4} ({percentage:5.1f}%) {bar}"
            )

        print("=" * 60 + "\n")

    def export_analysis_reports(self, results_path: Optional[Path] = None) -> None:
        """Export various analysis reports"""
        if not results_path:
            results_path = self.output_dir / "betfair_results.json"

        # Export incomplete matches
        incomplete_df = self.get_incomplete_matches(results_path)
        if not incomplete_df.empty:
            incomplete_path = self.output_dir / "incomplete_matches.csv"
            incomplete_df.to_csv(incomplete_path, index=False)
            logger.info(
                f"Exported {len(incomplete_df)} incomplete matches to {incomplete_path}"
            )

        # Export summary
        summary = self.get_market_coverage_summary(results_path=results_path)
        if summary:
            summary_path = self.output_dir / "market_coverage_summary.json"
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Exported market coverage summary to {summary_path}")

        # Export downloadable matches
        downloadable = self.get_downloadable_matches(results_path)
        if downloadable:
            download_path = self.output_dir / "ready_for_download.json"
            with open(download_path, "w") as f:
                json.dump(downloadable, f, indent=2, default=self._json_encoder)
            logger.info(
                f"Exported {len(downloadable)} downloadable matches to {download_path}"
            )
