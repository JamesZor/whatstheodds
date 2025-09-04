import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.betfair.downloader import BetfairDownloader
from whatstheodds.betfair.rate_limiter import RateLimitedContext

logger = logging.getLogger(__name__)


class DownloadStatus:
    """Track download status for reporting"""

    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    SKIPPED = "skipped"
    INVALID = "invalid"


@dataclass
class FileDownloadTask:
    """Represents a single file download task"""

    sofa_id: str
    betfair_id: str
    file_path: str
    save_folder: Path
    file_index: int
    total_files: int

    @property
    def filename(self) -> str:
        return Path(self.file_path).name


class BetfairBulkDownloader(BetfairDownloader):
    """
    Bulk downloader for Phase 2 - processes JSON output from BetfairDetailsGrabber
    Downloads files in parallel at the FILE level for maximum efficiency
    """

    def __init__(
        self,
        api_client=None,
        tmp_manager=None,
        cfg: Optional[DictConfig] = None,
        max_workers: int = 8,
        checkpoint_interval: int = 50,
    ):
        """
        Initialize bulk downloader

        Args:
            api_client: Betfair API client
            tmp_manager: Temp storage manager
            cfg: Configuration
            max_workers: Number of parallel download threads (default: 8)
            checkpoint_interval: Save progress every N files (default: 50)
        """
        super().__init__(api_client, tmp_manager, cfg)

        self.max_workers = max_workers
        self.checkpoint_interval = checkpoint_interval
        self.results_lock = Lock()

        # Track download session
        self.session_stats = {
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "invalid_files": 0,
            "skipped_files": 0,
            "session_start": datetime.now().isoformat(),
        }

        # Track match-level results
        self.match_results = {}

        logger.info(
            f"BetfairBulkDownloader initialized with {max_workers} workers (file-level parallelism)"
        )

    # ========== Main Entry Point ==========

    def download_from_phase1_json(
        self,
        json_path: Path,
        output_dir: Path,
        resume: bool = True,
        filter_complete_only: bool = True,
        retry_failed: bool = False,
    ) -> Dict[str, Any]:
        """
        Main entry point - download all files from Phase 1 JSON output

        Args:
            json_path: Path to JSON from BetfairDetailsGrabber
            output_dir: Root directory for downloads
            resume: Whether to resume from checkpoint
            filter_complete_only: Only download matches with status='complete'
            retry_failed: Whether to retry previously failed files

        Returns:
            Dict with download summary and statistics
        """
        # Load Phase 1 results
        logger.info(f"Loading Phase 1 results from: {json_path}")
        with open(json_path, "r") as f:
            phase1_data = json.load(f)

        # Extract matches
        matches = phase1_data.get("matches", {})
        metadata = phase1_data.get("metadata", {})

        logger.info(f"Found {len(matches)} total matches in Phase 1 data")

        # Filter for complete matches if requested
        if filter_complete_only:
            matches = {
                sofa_id: match_data
                for sofa_id, match_data in matches.items()
                if match_data.get("status") == "complete"
            }
            logger.info(f"Filtered to {len(matches)} complete matches")

        if not matches:
            logger.warning("No matches to download")
            return {"status": "no_matches", "summary": self.session_stats}

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Load checkpoint if resuming
        checkpoint_path = output_dir / "download_checkpoint.json"
        checkpoint = self._load_checkpoint(checkpoint_path) if resume else None

        # Build download tasks
        download_tasks = self._build_download_tasks(
            matches=matches,
            output_dir=output_dir,
            checkpoint=checkpoint,
            retry_failed=retry_failed,
        )

        if not download_tasks:
            logger.info("No files need downloading")
            return self._generate_final_summary(output_dir)

        logger.info(f"Total files to download: {len(download_tasks)}")

        # Initialize match results tracking
        self._initialize_match_results(matches, checkpoint)

        # Process all files with thread pool
        self._process_file_downloads(
            download_tasks=download_tasks, checkpoint_path=checkpoint_path
        )

        # Generate final summary
        return self._generate_final_summary(output_dir)

    # ========== Task Building ==========

    def _build_download_tasks(
        self,
        matches: Dict,
        output_dir: Path,
        checkpoint: Optional[Dict],
        retry_failed: bool,
    ) -> List[FileDownloadTask]:
        """
        Build list of all file download tasks

        Args:
            matches: Dict of matches from Phase 1
            output_dir: Root output directory
            checkpoint: Previous checkpoint data
            retry_failed: Whether to retry failed files

        Returns:
            List of FileDownloadTask objects
        """
        tasks = []
        processed_files = set()
        failed_files = set()

        # Extract processed and failed files from checkpoint
        if checkpoint:
            file_results = checkpoint.get("file_results", {})
            for file_path, result in file_results.items():
                if result["status"] == DownloadStatus.SUCCESS:
                    processed_files.add(file_path)
                elif result["status"] in [
                    DownloadStatus.FAILED,
                    DownloadStatus.INVALID,
                ]:
                    failed_files.add(file_path)

        # Build tasks for all files
        for sofa_id, match_data in matches.items():
            betfair_id = str(match_data.get("betfair_match_id"))
            if not betfair_id or betfair_id == "None":
                logger.warning(f"No betfair_id for match {sofa_id}")
                continue

            # Create match directory
            match_dir = output_dir / betfair_id
            match_dir.mkdir(exist_ok=True)

            # Get file list
            file_paths = match_data.get("market_files", [])
            if not file_paths:
                logger.warning(f"No files for match {sofa_id}")
                continue

            # Create task for each file
            for idx, file_path in enumerate(file_paths):
                # Skip if already processed successfully
                if file_path in processed_files:
                    # Check if file actually exists and is valid
                    local_path = match_dir / Path(file_path).name
                    if local_path.exists() and self._validate_downloaded_file(
                        local_path
                    ):
                        logger.debug(
                            f"Skipping already downloaded: {Path(file_path).name}"
                        )
                        continue

                # Skip failed files unless retry requested
                if file_path in failed_files and not retry_failed:
                    logger.debug(f"Skipping previously failed: {Path(file_path).name}")
                    continue

                # Create download task
                task = FileDownloadTask(
                    sofa_id=sofa_id,
                    betfair_id=betfair_id,
                    file_path=file_path,
                    save_folder=match_dir,
                    file_index=idx,
                    total_files=len(file_paths),
                )
                tasks.append(task)

        return tasks

    # ========== File Processing ==========

    def _process_file_downloads(
        self, download_tasks: List[FileDownloadTask], checkpoint_path: Path
    ) -> None:
        """
        Process all file downloads with thread pool

        Args:
            download_tasks: List of download tasks
            checkpoint_path: Path to save checkpoints
        """
        file_results = {}
        processed_count = 0

        # Load existing file results from checkpoint
        checkpoint = self._load_checkpoint(checkpoint_path)
        if checkpoint:
            file_results = checkpoint.get("file_results", {})

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self._download_single_file_task, task): task
                for task in download_tasks
            }

            # Process completed futures with progress bar
            with tqdm(total=len(futures), desc="Downloading files") as pbar:
                for future in as_completed(futures):
                    task = futures[future]

                    try:
                        # Get result with timeout
                        file_result = future.result(timeout=300)

                        # Update file results
                        file_results[task.file_path] = file_result

                        # Update match-level tracking
                        self._update_match_result(task, file_result)

                        # Update session stats
                        self._update_session_stats(file_result)

                        # Update progress bar
                        if file_result["status"] == DownloadStatus.SUCCESS:
                            pbar.set_postfix(status="✓", file=task.filename[:20])
                        elif file_result["status"] == DownloadStatus.SKIPPED:
                            pbar.set_postfix(status="⊙", file=task.filename[:20])
                        elif file_result["status"] == DownloadStatus.INVALID:
                            pbar.set_postfix(status="⚠", file=task.filename[:20])
                        else:
                            pbar.set_postfix(status="✗", file=task.filename[:20])

                        processed_count += 1

                        # Checkpoint save
                        if processed_count % self.checkpoint_interval == 0:
                            self._save_checkpoint(
                                checkpoint_path, file_results, self.match_results
                            )
                            logger.debug(
                                f"Checkpoint saved: {processed_count} files processed"
                            )

                    except Exception as e:
                        logger.error(f"Failed to process {task.filename}: {e}")
                        file_results[task.file_path] = {
                            "status": DownloadStatus.FAILED,
                            "error": str(e),
                            "file": task.filename,
                        }
                        self._update_match_result(task, file_results[task.file_path])

                    pbar.update(1)

        # Final checkpoint save
        self._save_checkpoint(checkpoint_path, file_results, self.match_results)
        logger.info(f"Download complete: processed {len(file_results)} files")

    def _download_single_file_task(self, task: FileDownloadTask) -> Dict:
        """
        Download a single file from a task

        Args:
            task: FileDownloadTask object

        Returns:
            Dict with download result
        """
        local_path = task.save_folder / task.filename

        # Check if already exists and is valid
        if local_path.exists():
            if self._validate_downloaded_file(local_path):
                return {
                    "status": DownloadStatus.SKIPPED,
                    "file": task.filename,
                    "sofa_id": task.sofa_id,
                    "betfair_id": task.betfair_id,
                    "reason": "Already exists and valid",
                }
            else:
                # Remove invalid file
                local_path.unlink(missing_ok=True)

        # Download with rate limiting
        try:
            with RateLimitedContext():
                # Use parent class download method
                self.client.historic.download_file(
                    file_path=task.file_path, store_directory=str(task.save_folder)
                )

            # Validate downloaded file
            if self._validate_downloaded_file(local_path):
                return {
                    "status": DownloadStatus.SUCCESS,
                    "file": task.filename,
                    "sofa_id": task.sofa_id,
                    "betfair_id": task.betfair_id,
                    "size": local_path.stat().st_size,
                }
            else:
                # Remove invalid file
                local_path.unlink(missing_ok=True)
                return {
                    "status": DownloadStatus.INVALID,
                    "file": task.filename,
                    "sofa_id": task.sofa_id,
                    "betfair_id": task.betfair_id,
                    "error": "Failed validation (HTML/corrupted)",
                }

        except Exception as e:
            logger.debug(f"Download failed for {task.filename}: {e}")

            # Check if it's an auth issue
            if "401" in str(e) or "unauthorized" in str(e).lower():
                # Try reauthentication
                if self._reauthenticate():
                    # Retry once after reauth
                    try:
                        with RateLimitedContext():
                            self.client.historic.download_file(
                                file_path=task.file_path,
                                store_directory=str(task.save_folder),
                            )

                        if self._validate_downloaded_file(local_path):
                            return {
                                "status": DownloadStatus.SUCCESS,
                                "file": task.filename,
                                "sofa_id": task.sofa_id,
                                "betfair_id": task.betfair_id,
                                "size": local_path.stat().st_size,
                                "note": "Required reauthentication",
                            }
                    except Exception as retry_error:
                        pass

            return {
                "status": DownloadStatus.FAILED,
                "file": task.filename,
                "sofa_id": task.sofa_id,
                "betfair_id": task.betfair_id,
                "error": str(e),
            }

    # ========== Match Result Tracking ==========

    def _initialize_match_results(
        self, matches: Dict, checkpoint: Optional[Dict]
    ) -> None:
        """Initialize match-level result tracking"""
        # Start fresh or load from checkpoint
        if checkpoint and "match_results" in checkpoint:
            self.match_results = checkpoint["match_results"]
        else:
            self.match_results = {}

        # Initialize tracking for each match
        for sofa_id, match_data in matches.items():
            if sofa_id not in self.match_results:
                self.match_results[sofa_id] = {
                    "betfair_id": match_data.get("betfair_match_id"),
                    "total_files": len(match_data.get("market_files", [])),
                    "downloaded": 0,
                    "failed": 0,
                    "invalid": 0,
                    "skipped": 0,
                    "status": DownloadStatus.FAILED,
                    "home": match_data.get("home"),
                    "away": match_data.get("away"),
                    "date": match_data.get("date"),
                }

    def _update_match_result(self, task: FileDownloadTask, file_result: Dict) -> None:
        """Update match-level results based on file result"""
        with self.results_lock:
            if task.sofa_id not in self.match_results:
                self.match_results[task.sofa_id] = {
                    "betfair_id": task.betfair_id,
                    "total_files": task.total_files,
                    "downloaded": 0,
                    "failed": 0,
                    "invalid": 0,
                    "skipped": 0,
                    "status": DownloadStatus.FAILED,
                }

            match_result = self.match_results[task.sofa_id]

            # Update counters
            if file_result["status"] == DownloadStatus.SUCCESS:
                match_result["downloaded"] += 1
            elif file_result["status"] == DownloadStatus.SKIPPED:
                match_result["skipped"] += 1
            elif file_result["status"] == DownloadStatus.INVALID:
                match_result["invalid"] += 1
            else:
                match_result["failed"] += 1

            # Update match status
            total_successful = match_result["downloaded"] + match_result["skipped"]

            if total_successful == match_result["total_files"]:
                match_result["status"] = DownloadStatus.SUCCESS
            elif total_successful > 0:
                match_result["status"] = DownloadStatus.PARTIAL
            else:
                match_result["status"] = DownloadStatus.FAILED

    def _update_session_stats(self, file_result: Dict) -> None:
        """Update session-level statistics"""
        with self.results_lock:
            self.session_stats["total_files"] += 1

            if file_result["status"] == DownloadStatus.SUCCESS:
                self.session_stats["downloaded_files"] += 1
            elif file_result["status"] == DownloadStatus.FAILED:
                self.session_stats["failed_files"] += 1
            elif file_result["status"] == DownloadStatus.INVALID:
                self.session_stats["invalid_files"] += 1
            elif file_result["status"] == DownloadStatus.SKIPPED:
                self.session_stats["skipped_files"] += 1

    # ========== Checkpoint Management ==========

    def _save_checkpoint(
        self, checkpoint_path: Path, file_results: Dict, match_results: Dict
    ) -> None:
        """Save checkpoint for resuming"""
        checkpoint = {
            "last_updated": datetime.now().isoformat(),
            "file_results": file_results,
            "match_results": match_results,
            "session_stats": self.session_stats,
        }

        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint, f, indent=2, default=str)

    def _load_checkpoint(self, checkpoint_path: Path) -> Optional[Dict]:
        """Load checkpoint if exists"""
        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path, "r") as f:
                checkpoint = json.load(f)

            files_processed = len(checkpoint.get("file_results", {}))
            logger.info(f"Loaded checkpoint: {files_processed} files already processed")
            return checkpoint
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None

    # ========== Summary and Reporting ==========

    def _generate_final_summary(self, output_dir: Path) -> Dict:
        """Generate final summary report"""
        # Count match statuses
        successful_matches = sum(
            1
            for m in self.match_results.values()
            if m["status"] == DownloadStatus.SUCCESS
        )
        partial_matches = [
            sofa_id
            for sofa_id, m in self.match_results.items()
            if m["status"] == DownloadStatus.PARTIAL
        ]
        failed_matches = [
            sofa_id
            for sofa_id, m in self.match_results.items()
            if m["status"] == DownloadStatus.FAILED
        ]

        summary = {
            "session_start": self.session_stats["session_start"],
            "session_end": datetime.now().isoformat(),
            "total_matches": len(self.match_results),
            "successful_matches": successful_matches,
            "partial_matches": partial_matches,
            "failed_matches": failed_matches,
            "file_statistics": self.session_stats,
            "match_details": self.match_results,
        }

        # Save summary
        summary_path = output_dir / "download_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved to: {summary_path}")

        # Generate retry lists
        if partial_matches:
            self._generate_retry_list(output_dir, partial_matches, "retry_partial.json")

        if failed_matches:
            self._generate_retry_list(output_dir, failed_matches, "retry_failed.json")

        # Print summary
        self._print_download_summary(summary)

        return summary

    def _generate_retry_list(
        self, output_dir: Path, match_ids: List[str], filename: str
    ) -> None:
        """Generate retry list for failed/partial matches"""
        retry_data = {"generated": datetime.now().isoformat(), "matches": {}}

        for sofa_id in match_ids:
            match_result = self.match_results.get(sofa_id, {})
            retry_data["matches"][sofa_id] = {
                "betfair_id": match_result.get("betfair_id"),
                "status": match_result.get("status"),
                "total_files": match_result.get("total_files"),
                "downloaded": match_result.get("downloaded"),
                "failed": match_result.get("failed"),
                "invalid": match_result.get("invalid"),
            }

        retry_path = output_dir / filename
        with open(retry_path, "w") as f:
            json.dump(retry_data, f, indent=2)

        logger.info(f"Generated retry list ({len(match_ids)} matches): {retry_path}")

    def _print_download_summary(self, summary: Dict) -> None:
        """Print final download summary"""
        print("\n" + "=" * 60)
        print("BULK DOWNLOAD SUMMARY")
        print("=" * 60)

        print(f"\nSession Statistics:")
        print(
            f"  Duration: {summary['session_start'][:19]} to {summary['session_end'][:19]}"
        )

        print(f"\nMatch Statistics:")
        print(f"  Total matches: {summary['total_matches']}")
        print(
            f"  Successful: {summary['successful_matches']} ({summary['successful_matches']/max(1,summary['total_matches'])*100:.1f}%)"
        )
        print(f"  Partial: {len(summary['partial_matches'])}")
        print(f"  Failed: {len(summary['failed_matches'])}")

        file_stats = summary["file_statistics"]
        total = file_stats.get("total_files", 0)
        print(f"\nFile Statistics:")
        print(f"  Total files: {total}")
        print(f"  Downloaded: {file_stats.get('downloaded_files', 0)}")
        print(f"  Skipped (exists): {file_stats.get('skipped_files', 0)}")
        print(f"  Invalid: {file_stats.get('invalid_files', 0)}")
        print(f"  Failed: {file_stats.get('failed_files', 0)}")

        if total > 0:
            success_rate = (
                (
                    file_stats.get("downloaded_files", 0)
                    + file_stats.get("skipped_files", 0)
                )
                / total
                * 100
            )
            print(f"  Success rate: {success_rate:.1f}%")

        if summary["partial_matches"]:
            print(
                f"\n⚠ {len(summary['partial_matches'])} matches have partial downloads"
            )
            print("  See retry_partial.json for details")

        if summary["failed_matches"]:
            print(f"\n✗ {len(summary['failed_matches'])} matches failed completely")
            print("  See retry_failed.json for details")

        print("=" * 60 + "\n")

    # ========== Batch Processing for Large Datasets ==========

    def process_season_batch(
        self, json_paths: List[Path], output_dir: Path, batch_name: str = "season"
    ) -> Dict:
        """
        Process multiple JSON files (e.g., entire season)

        Args:
            json_paths: List of Phase 1 JSON files
            output_dir: Root output directory
            batch_name: Name for this batch (for logging)

        Returns:
            Combined summary for all files
        """
        logger.info(f"Starting batch processing: {batch_name}")
        logger.info(f"Processing {len(json_paths)} JSON files")

        batch_results = {}

        for i, json_path in enumerate(json_paths, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing file {i}/{len(json_paths)}: {json_path.name}")
            logger.info(f"{'='*60}")

            # Process this JSON file
            results = self.download_from_phase1_json(
                json_path=json_path,
                output_dir=output_dir,  # Use same output dir for all
                resume=True,
                filter_complete_only=True,
            )

            batch_results[str(json_path)] = results

        # Save batch summary
        batch_summary_path = output_dir / f"{batch_name}_batch_summary.json"
        with open(batch_summary_path, "w") as f:
            json.dump(batch_results, f, indent=2, default=str)

        logger.info(
            f"\nBatch processing complete. Summary saved to: {batch_summary_path}"
        )

        # Print combined statistics
        total_matches = sum(r.get("total_matches", 0) for r in batch_results.values())
        total_successful = sum(
            r.get("successful_matches", 0) for r in batch_results.values()
        )
        total_files = sum(
            r.get("file_statistics", {}).get("total_files", 0)
            for r in batch_results.values()
        )
        total_downloaded = sum(
            r.get("file_statistics", {}).get("downloaded_files", 0)
            for r in batch_results.values()
        )

        print(f"\nBatch Summary for '{batch_name}':")
        print(f"  Files processed: {len(json_paths)}")
        print(f"  Total matches: {total_matches}")
        print(f"  Successful matches: {total_successful}")
        print(f"  Total files: {total_files}")
        print(f"  Downloaded files: {total_downloaded}")

        return batch_results

    def get_download_statistics(self, output_dir: Path) -> Dict:
        """Get statistics from a download directory"""
        summary_path = output_dir / "download_summary.json"
        if not summary_path.exists():
            return {}

        with open(summary_path, "r") as f:
            return json.load(f)


# ========== Example Usage ==========
if __name__ == "__main__":
    from pathlib import Path

    # Initialize downloader with more workers for file-level parallelism
    downloader = BetfairBulkDownloader(
        max_workers=8,  # Can be higher since we're threading on files
        checkpoint_interval=50,  # Save every 50 files
    )

    # Single JSON file
    results = downloader.download_from_phase1_json(
        json_path=Path("betfair_results.json"),
        output_dir=Path("downloads/"),
        resume=True,
        filter_complete_only=True,
        retry_failed=False,  # Set True to retry failed files
    )

    # Multiple files (entire season)
    # season_files = [
    #     Path("scottish_prem_2024.json"),
    #     Path("scottish_champ_2024.json"),
    # ]
    #
    # batch_results = downloader.process_season_batch(
    #     json_paths=season_files,
    #     output_dir=Path("downloads/scottish_2024/"),
    #     batch_name="scottish_2024"
    # )
