import json
import logging
import multiprocessing
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.extractors.match_extractor import MatchExtractor
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


class BulkOddsExtractor:
    """
    Bulk extraction of odds data from downloaded Betfair files
    Uses multiprocessing for CPU-efficient parallel extraction
    """

    def __init__(
        self,
        downloads_dir: Path,
        n_processes: int = 4,
        batch_size: int = 100,
        cfg: Optional[DictConfig] = None,
    ):
        """
        Initialize bulk odds extractor

        Args:
            downloads_dir: Root directory where files were downloaded
            n_processes: Number of CPU processes for parallel extraction
            batch_size: Number of matches to process per batch (memory management)
            cfg: Configuration object
        """
        self.downloads_dir = Path(downloads_dir)
        self.n_processes = n_processes
        self.batch_size = batch_size

        if cfg is None:
            cfg = load_config(config_dir="configs", config_file="history_test.yaml")
        self.cfg = cfg

        # Extraction statistics
        self.stats: Dict = {
            "total_matches": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "total_data_points": 0,
            "extraction_start": None,
            "failed_matches": [],
        }

        logger.info("BulkOddsExtractor initialized")
        logger.info(f"  Downloads dir: {self.downloads_dir}")
        logger.info(f"  Processes: {self.n_processes}")
        logger.info(f"  Batch size: {self.batch_size}")

    # ========== Main Entry Point ==========

    def extract_from_downloads(
        self,
        phase1_json: Path,
        output_csv: Path,
        download_summary: Optional[Path] = None,
        filter_complete_only: bool = True,
        checkpoint_interval: int = 50,
        resume: bool = True,
    ) -> Dict[str, Any]:
        """
        Extract odds from all downloaded matches and combine into single CSV

        Args:
            phase1_json: Path to Phase 1 JSON with match mappings
            output_csv: Path for combined output CSV
            download_summary: Optional path to download summary (to filter complete downloads)
            filter_complete_only: Only process successfully downloaded matches
            checkpoint_interval: Save progress every N matches
            resume: Resume from checkpoint if available

        Returns:
            Summary dict with extraction statistics
        """
        logger.info("=" * 60)
        logger.info("Starting Bulk Odds Extraction")
        logger.info("=" * 60)

        self.stats["extraction_start"] = datetime.now().isoformat()

        # Load match metadata
        matches_to_process = self._load_matches_to_process(
            phase1_json, download_summary, filter_complete_only
        )

        if not matches_to_process:
            logger.warning("No matches to process")
            return self._generate_summary(output_csv)

        self.stats["total_matches"] = len(matches_to_process)
        logger.info(f"Found {len(matches_to_process)} matches to process")

        # Check for existing checkpoint
        checkpoint_path = output_csv.parent / f"{output_csv.stem}_checkpoint.json"
        processed_matches = set()

        if resume and checkpoint_path.exists():
            processed_matches = self._load_checkpoint(checkpoint_path)
            logger.info(f"Resuming: {len(processed_matches)} matches already processed")

        # Filter out already processed matches
        matches_to_process = [
            m
            for m in matches_to_process
            if m[0] not in processed_matches  # m[0] is sofa_id
        ]

        if not matches_to_process:
            logger.info("All matches already processed")
            return self._generate_summary(output_csv)

        # Process in batches
        self._process_all_matches(
            matches_to_process,
            output_csv,
            checkpoint_path,
            checkpoint_interval,
            resume and output_csv.exists(),  # append mode if resuming
        )

        # Generate and save summary
        return self._generate_summary(output_csv)

    # ========== Match Loading ==========

    def _load_matches_to_process(
        self,
        phase1_json: Path,
        download_summary: Optional[Path],
        filter_complete_only: bool,
    ) -> List[Tuple[str, str, Dict]]:
        """
        Load list of matches to process

        Returns:
            List of tuples: (sofa_id, betfair_id, match_metadata)
        """
        # Load Phase 1 results
        with open(phase1_json, "r") as f:
            phase1_data = json.load(f)

        matches = phase1_data.get("matches", {})

        # Filter by download status if summary provided
        if filter_complete_only and download_summary and download_summary.exists():
            with open(download_summary, "r") as f:
                download_data = json.load(f)

            # Get successfully downloaded matches
            successful_matches = {
                sofa_id
                for sofa_id, details in download_data.get("match_details", {}).items()
                if details.get("status") == "success"
            }

            # Filter matches
            matches = {
                sofa_id: data
                for sofa_id, data in matches.items()
                if sofa_id in successful_matches
            }

            logger.info(f"Filtered to {len(matches)} successfully downloaded matches")

        # Convert to list of tuples
        matches_list = []
        for sofa_id, match_data in matches.items():
            betfair_id = str(match_data.get("betfair_match_id"))

            # Skip if no betfair_id
            if not betfair_id or betfair_id == "None":
                logger.warning(f"Skipping match {sofa_id}: No betfair_id")
                continue

            # Check if directory exists
            match_dir = self.downloads_dir / betfair_id
            if not match_dir.exists():
                logger.warning(
                    f"Skipping match {sofa_id}: Directory not found: {match_dir}"
                )
                continue

            matches_list.append((sofa_id, betfair_id, match_data))

        return matches_list

    # ========== Batch Processing ==========

    def _process_all_matches(
        self,
        matches: List[Tuple[str, str, Dict]],
        output_csv: Path,
        checkpoint_path: Path,
        checkpoint_interval: int,
        append_mode: bool,
    ) -> None:
        """
        Process all matches in batches

        Args:
            matches: List of (sofa_id, betfair_id, metadata) tuples
            output_csv: Output CSV path
            checkpoint_path: Checkpoint file path
            checkpoint_interval: Save frequency
            append_mode: Whether to append to existing file
        """
        total_batches = (len(matches) + self.batch_size - 1) // self.batch_size
        logger.info(f"Processing {len(matches)} matches in {total_batches} batches")

        processed_count = 0
        header_written = append_mode  # Don't write header if appending
        processed_matches = []

        # Process in batches
        for batch_idx in range(0, len(matches), self.batch_size):
            batch = matches[batch_idx : batch_idx + self.batch_size]
            batch_num = batch_idx // self.batch_size + 1

            logger.info(
                f"\nProcessing batch {batch_num}/{total_batches} ({len(batch)} matches)"
            )

            # Extract batch in parallel
            batch_results = self._process_batch_parallel(batch)

            if batch_results:
                # Combine batch DataFrames
                batch_df = pd.concat(batch_results, ignore_index=True)

                # Add to output file
                self._save_to_csv(batch_df, output_csv, header_written)
                header_written = True

                # Update statistics
                self.stats["successful_extractions"] += len(batch_results)
                self.stats["total_data_points"] += len(batch_df)

                # Track processed matches
                processed_matches.extend(
                    [
                        m[0]
                        for m in batch
                        if self._match_was_processed(m[0], batch_results)
                    ]
                )
                processed_count += len(batch_results)

                # Checkpoint if needed
                if processed_count % checkpoint_interval == 0:
                    self._save_checkpoint(checkpoint_path, processed_matches)
                    logger.info(
                        f"Checkpoint saved: {processed_count} matches processed"
                    )

            logger.info(
                f"Batch {batch_num} complete: {len(batch_results)}/{len(batch)} successful"
            )

        # Final checkpoint
        if processed_matches:
            self._save_checkpoint(checkpoint_path, processed_matches)

    def _process_batch_parallel(
        self, batch: List[Tuple[str, str, Dict]]
    ) -> List[pd.DataFrame]:
        """
        Process a batch of matches in parallel using multiprocessing

        Args:
            batch: List of (sofa_id, betfair_id, metadata) tuples

        Returns:
            List of DataFrames (one per successful extraction)
        """
        # Create partial function with fixed arguments
        extract_func = partial(
            extract_single_match_worker, downloads_dir=self.downloads_dir, cfg=self.cfg
        )

        # Process in parallel
        with multiprocessing.Pool(self.n_processes) as pool:
            # Use tqdm for progress bar
            results = list(
                tqdm(
                    pool.imap(extract_func, batch),
                    total=len(batch),
                    desc="Extracting matches",
                    unit="match",
                )
            )

        # Filter out None results (failed extractions)
        successful_results = []
        for i, result in enumerate(results):
            if result is not None:
                successful_results.append(result)
            else:
                # Track failure
                sofa_id, betfair_id, _ = batch[i]
                self.stats["failed_matches"].append(
                    {
                        "sofa_id": sofa_id,
                        "betfair_id": betfair_id,
                        "error": "Extraction failed",
                    }
                )
                self.stats["failed_extractions"] += 1

        return successful_results

    def _match_was_processed(
        self, sofa_id: str, batch_results: List[pd.DataFrame]
    ) -> bool:
        """Check if a match was successfully processed"""
        for df in batch_results:
            if not df.empty and "sofa_id" in df.columns:
                if sofa_id in df["sofa_id"].values:
                    return True
        return False

    # ========== File I/O ==========

    def _save_to_csv(
        self, df: pd.DataFrame, output_path: Path, header_written: bool
    ) -> None:
        """Save DataFrame to CSV, appending if file exists"""
        mode = "a" if header_written else "w"
        header = not header_written

        df.to_csv(output_path, mode=mode, header=header, index=False)
        logger.debug(f"Saved {len(df)} rows to {output_path}")

    def _save_checkpoint(
        self, checkpoint_path: Path, processed_matches: List[str]
    ) -> None:
        """Save checkpoint for resuming"""
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            "processed_count": len(processed_matches),
            "processed_matches": processed_matches,
        }

        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint, f, indent=2)

    def _load_checkpoint(self, checkpoint_path: Path) -> set:
        """Load checkpoint and return set of processed match IDs"""
        try:
            with open(checkpoint_path, "r") as f:
                checkpoint = json.load(f)
            return set(checkpoint.get("processed_matches", []))
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return set()

    # ========== Summary Generation ==========

    def _generate_summary(self, output_csv: Path) -> Dict[str, Any]:
        """Generate extraction summary"""
        self.stats["extraction_end"] = datetime.now().isoformat()
        self.stats["output_file"] = str(output_csv)

        # Calculate success rate
        total = self.stats["total_matches"]
        successful = self.stats["successful_extractions"]
        self.stats["success_rate"] = (successful / total * 100) if total > 0 else 0

        # Save summary
        summary_path = output_csv.parent / f"{output_csv.stem}_extraction_summary.json"
        with open(summary_path, "w") as f:
            json.dump(self.stats, f, indent=2)

        # Print summary
        self._print_summary()

        return self.stats

    def _print_summary(self) -> None:
        """Print extraction summary"""
        print("\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)

        print(f"\nStatistics:")
        print(f"  Total matches: {self.stats['total_matches']}")
        print(f"  Successful: {self.stats['successful_extractions']}")
        print(f"  Failed: {self.stats['failed_extractions']}")
        print(f"  Success rate: {self.stats.get('success_rate', 0):.1f}%")
        print(f"  Total data points: {self.stats['total_data_points']:,}")

        if self.stats["failed_matches"]:
            print(
                f"\n  ⚠ {len(self.stats['failed_matches'])} matches failed extraction"
            )
            print("  See extraction_summary.json for details")

        print("=" * 60 + "\n")


# ========== Worker Function for Multiprocessing ==========


def extract_single_match_worker(
    match_data: Tuple[str, str, Dict], downloads_dir: Path, cfg: DictConfig
) -> Optional[pd.DataFrame]:
    """
    Worker function to extract a single match
    Must be at module level for multiprocessing

    Args:
        match_data: Tuple of (sofa_id, betfair_id, metadata)
        downloads_dir: Root downloads directory
        cfg: Configuration

    Returns:
        DataFrame with extracted odds or None if failed
    """
    sofa_id, betfair_id, metadata = match_data

    try:
        # Create extractor for this process
        extractor = MatchExtractor(cfg)

        # Override temp directory to point to downloads
        extractor.temp_dir = downloads_dir

        # Extract match
        result = extractor.extract_match(betfair_id)

        if result and "odds_data" in result:
            df = result["odds_data"]

            # Add match identifiers
            df["match_id"] = sofa_id
            df["betfair_id"] = betfair_id

            # Add metadata if needed
            # df["home_team"] = metadata.get("home", "")
            # df["away_team"] = metadata.get("away", "")
            # df["match_date"] = metadata.get("date", "")

            return df
        else:
            logger.warning(f"No odds data extracted for {sofa_id}")
            return None

    except Exception as e:
        logger.error(f"Error extracting match {sofa_id}: {e}")
        return None


# ========== Utility Functions ==========


def combine_csv_files(csv_files: List[Path], output_path: Path) -> None:
    """
    Utility to combine multiple CSV files into one

    Args:
        csv_files: List of CSV file paths
        output_path: Output combined CSV path
    """
    dfs = []
    for csv_file in csv_files:
        if csv_file.exists():
            df = pd.read_csv(csv_file)
            dfs.append(df)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined.to_csv(output_path, index=False)
        logger.info(f"Combined {len(dfs)} files into {output_path}")
        logger.info(f"Total rows: {len(combined):,}")


# ========== Example Usage ==========

# if __name__ == "__main__":
#     from pathlib import Path
#
#     # Initialize extractor
#     extractor = BulkOddsExtractor(
#         downloads_dir=Path(
#             "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/"
#         ),
#         n_processes=6,  # Use 6 CPU cores
#         batch_size=50,  # Process 50 matches at a time
#     )
#
#     # Extract all odds
#     summary = extractor.extract_from_downloads(
#         phase1_json=Path(
#             "/home/james/bet_project/football_data/test/threaded_test.json"
#         ),
#         output_csv=Path("/home/james/bet_project/football_data/test/all_odds.csv"),
#         download_summary=Path(
#             "/home/james/bet_project/football_data/test/bulk_test_downloads_v1/download_summary.json"
#         ),
#         filter_complete_only=True,
#         resume=True,
#     )
#
#     print(f"\nExtraction complete!")
#     print(f"Output: {summary['output_file']}")
#     print(f"Total data points: {summary['total_data_points']:,}")
