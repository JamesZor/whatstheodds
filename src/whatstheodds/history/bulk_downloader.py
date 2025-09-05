import bz2
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

# --- Assumed Local Imports ---
# These imports are based on the original files. You may need to adjust the paths.
from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.betfair.rate_limiter import RateLimitedContext, betfair_rate_limiter
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )


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

    @property
    def filename(self) -> str:
        return Path(self.file_path).name


class BetfairBulkDownloader:
    """
    A robust, parallel downloader for Betfair historical data.

    This class acts as an orchestrator, processing large download jobs in manageable
    batches. It uses a 3-layered session management strategy to ensure a stable
    connection: a background keep-alive thread, a failsafe re-authentication
    check before each batch, and simple, stateless worker threads for downloads.
    """

    def __init__(
        self,
        cfg: Optional[DictConfig] = None,
        max_workers: int = 8,
        checkpoint_interval: int = 50,
        show_rate_limit_bar: bool = True,
    ):
        """
        Initializes the BetfairBulkDownloader.

        Args:
            cfg: Configuration object. If None, it will be loaded automatically.
            max_workers: The number of parallel download threads to use per batch.
            checkpoint_interval: How often to save progress (number of files).
            show_rate_limit_bar: Whether to display a TQDM bar for API rate limiting.
        """
        # 1. Load configuration and initialize shared API client
        self.cfg: DictConfig = load_config() if cfg is None else cfg
        self.client: betfairlightweight.APIClient = setup_betfair_api_client()

        # 2. Initialize threading and session management attributes
        self._auth_lock = threading.Lock()
        self._keep_alive_thread: Optional[threading.Thread] = None
        self._keep_alive_active = threading.Event()
        self._monitoring_active = False

        # 3. Initialize downloader settings and tracking dictionaries
        self.max_workers = max_workers
        self.checkpoint_interval = checkpoint_interval
        self.show_rate_limit_bar = show_rate_limit_bar

        self.results_lock = Lock()
        self.match_results: Dict[str, Any] = {}
        self.session_stats: Dict[str, Any] = {}
        self._reset_session_stats()

    def _reset_session_stats(self):
        """Resets the session statistics tracker."""
        self.session_stats = {
            "total_files_in_job": 0,
            "processed_files_in_session": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "invalid_files": 0,
            "skipped_files": 0,
            "session_start": datetime.now().isoformat(),
        }

    # =========================================================================
    # 3-Layered Session Management
    # =========================================================================

    def _keep_alive_worker(self, interval_minutes: int = 1):
        """
        The Heartbeat (Layer 1): Runs in a background thread to prevent the
        session token from expiring due to inactivity.
        """
        interval_seconds = interval_minutes * 60
        logger.info(
            f"Keep-alive thread started. Will refresh session every {interval_minutes} minutes."
        )

        while not self._keep_alive_active.wait(timeout=interval_seconds):
            try:
                logger.info("Keep-alive thread: Calling keep_alive...")
                self.client.keep_alive()
                logger.info("Keep-alive thread: Session extended successfully.")
            except Exception as e:
                logger.error(f"Keep-alive thread encountered an error: {e}")

        logger.info("Keep-alive thread finished.")

    def _reauthenticate(self) -> bool:
        """
        The Failsafe (Layer 2): Ensures the session is valid before starting
        a new batch. It first attempts a low-cost keep_alive call. If that fails,
        it performs a full, fresh login.
        """
        try:
            # First, try the cheap keep-alive call.
            self.client.keep_alive()
            logger.info("Session is active. Extended successfully via pre-batch check.")
            return True
        except Exception:
            logger.warning(
                "Keep-alive failed. Session may have expired. Attempting fresh login."
            )

        # If keep_alive failed, proceed with a full, thread-safe re-login.
        with self._auth_lock:
            try:
                if hasattr(self.client, "session_token") and self.client.session_token:
                    try:
                        self.client.logout()
                    except Exception as e:
                        logger.warning(f"Ignoring error during logout: {e}")

                logger.info("Attempting interactive login...")
                self.client.login_interactive()
                logger.info("Successfully created a new session token.")
                return True
            except Exception as e:
                logger.error(f"Full re-login attempt failed: {e}")
                return False

    def _download_with_retries(self, task: FileDownloadTask) -> Path:
        """
        The Simple Worker (Layer 3): Handles the download for a single file with
        simple, stateless retries for transient network issues. It does NOT
        handle session management.
        """
        max_retries = self.cfg.betfair_downloader.get("html_retries", 3)
        sleep_duration = self.cfg.betfair_downloader.get("html_sleep_before_retry", 3)

        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.debug(f"Retry {attempt}/{max_retries} for {task.filename}")
                time.sleep(sleep_duration + random.uniform(0, 1))  # Add jitter

            result = self._attempt_download(task)
            if result:
                return result  # Success

        raise Exception(f"All download attempts failed for {task.filename}")

    # =========================================================================
    # Core Orchestration and Download Logic
    # =========================================================================
    def _create_rate_limit_monitor(self, position: int = 1) -> Optional[tqdm]:
        """Creates a separate progress bar to monitor API rate limit usage."""
        if not self.show_rate_limit_bar:
            return None

        rate_bar = tqdm(
            total=100,
            desc="Rate Limit",
            position=position,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {postfix}",
            leave=False,  # Set back to False for a cleaner exit
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

    def _process_file_downloads(
        self, download_tasks: List[FileDownloadTask], checkpoint_path: Path
    ) -> None:
        """
        The Orchestrator (Non-Batching Version): Manages the download process by
        submitting all tasks to a single thread pool for maximum throughput.
        """
        if not download_tasks:
            logger.info("No files to download.")
            return

        # 1. Start the Heartbeat Thread (now more important than ever)
        self._keep_alive_active.clear()
        self._keep_alive_thread = threading.Thread(
            target=self._keep_alive_worker, daemon=True
        )
        self._keep_alive_thread.start()

        processed_count = 0
        file_results = self._load_checkpoint(checkpoint_path).get("file_results", {})

        rate_monitor = self._create_rate_limit_monitor(position=1)
        pbar_position = 0 if rate_monitor else None

        try:
            # 2. Perform a single, upfront session validation before starting.
            logger.info("Performing upfront session validation...")
            if not self._reauthenticate():
                logger.error(
                    "Initial session validation failed. Aborting download job."
                )
                # Stop the keep-alive thread if we abort early
                self._keep_alive_active.set()
                if rate_monitor:
                    self._monitoring_active = False
                return

            with tqdm(
                total=len(download_tasks),
                desc="Downloading files",
                position=pbar_position,
            ) as pbar:
                # 3. Create a single ThreadPoolExecutor for all tasks.
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit ALL tasks to the pool at once.
                    futures = {
                        executor.submit(self.download_single_file_task, task): task
                        for task in download_tasks
                    }

                    # 4. Process futures as they complete, maximizing throughput.
                    for future in as_completed(futures):
                        try:
                            task = futures[future]
                            result = future.result()

                            file_results[task.file_path] = result
                            self._update_match_result(task, result)
                            self._update_session_stats(result)

                            status_map = {
                                DownloadStatus.SUCCESS: "✓",
                                DownloadStatus.SKIPPED: "⊙",
                                DownloadStatus.INVALID: "⚠",
                                DownloadStatus.FAILED: "✗",
                            }
                            status_icon = status_map.get(result["status"], "✗")

                            pbar.set_postfix(
                                {"status": status_icon, "file": task.filename[:25]}
                            )
                            pbar.update(1)
                            processed_count += 1

                            if processed_count % self.checkpoint_interval == 0:
                                self._save_checkpoint(checkpoint_path, file_results)

                        except Exception as e:
                            logger.error(f"A task failed unexpectedly: {e}")
        finally:
            # 5. Gracefully stop background threads and save final checkpoint.
            logger.info("Download process finished. Stopping background threads...")
            self._keep_alive_active.set()

            if rate_monitor:
                self._monitoring_active = False
                time.sleep(0.6)

            pbar.close()
            self._save_checkpoint(checkpoint_path, file_results)
            logger.info(
                f"Final checkpoint saved. {processed_count} files processed in this session."
            )

    #
    # def _process_file_downloads(
    #     self, download_tasks: List[FileDownloadTask], checkpoint_path: Path
    # ) -> None:
    #     """
    #     The Orchestrator: Manages the entire download process, including the
    #     heartbeat thread, chunking tasks, and calling the failsafe between batches.
    #     """
    #     if not download_tasks:
    #         logger.info("No files to download.")
    #         return
    #
    #     # Start the Heartbeat Thread
    #     self._keep_alive_active.clear()
    #     self._keep_alive_thread = threading.Thread(
    #         target=self._keep_alive_worker, daemon=True
    #     )
    #     self._keep_alive_thread.start()
    #
    #     processed_count = 0
    #     file_results = self._load_checkpoint(checkpoint_path).get("file_results", {})
    #
    #     rate_monitor = self._create_rate_limit_monitor(position=1)
    #     pbar_position = 0 if rate_monitor else None
    #
    #     try:
    #         # Split tasks into manageable chunks
    #         chunk_size = self.max_workers * self.cfg.betfair_downloader.get(
    #             "batch_multiplier", 5
    #         )
    #         task_chunks = [
    #             download_tasks[i : i + chunk_size]
    #             for i in range(0, len(download_tasks), chunk_size)
    #         ]
    #         logger.info(
    #             f"Processing {len(download_tasks)} files in {len(task_chunks)} batches."
    #         )
    #
    #         with tqdm(
    #             total=len(download_tasks),
    #             desc="Downloading files",
    #             position=pbar_position,
    #         ) as pbar:
    #             # Loop through each chunk sequentially
    #             for i, chunk in enumerate(task_chunks):
    #                 pbar.set_description(f"Batch {i+1}/{len(task_chunks)}")
    #
    #                 # Call the Failsafe before starting the batch
    #                 if not self._reauthenticate():
    #                     logger.error(
    #                         "Session re-authentication failed. Aborting download job."
    #                     )
    #                     break
    #
    #                 # Process the current chunk in a new, isolated ThreadPoolExecutor
    #                 with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    #                     futures = {
    #                         executor.submit(self.download_single_file_task, task): task
    #                         for task in chunk
    #                     }
    #                     for future in as_completed(futures):
    #                         try:
    #                             task = futures[future]
    #                             result = future.result()
    #
    #                             # Update tracking and results
    #                             file_results[task.file_path] = result
    #                             self._update_match_result(task, result)
    #                             self._update_session_stats(result)
    #
    #                             # Set status icon for the progress bar
    #                             status_map = {
    #                                 DownloadStatus.SUCCESS: "✓",
    #                                 DownloadStatus.SKIPPED: "⊙",
    #                                 DownloadStatus.INVALID: "⚠",
    #                                 DownloadStatus.FAILED: "✗",
    #                             }
    #                             status_icon = status_map.get(result["status"], "✗")
    #
    #                             pbar.set_postfix(
    #                                 {"status": status_icon, "file": task.filename[:25]}
    #                             )
    #                             pbar.update(1)
    #                             processed_count += 1
    #
    #                             if processed_count % self.checkpoint_interval == 0:
    #                                 self._save_checkpoint(checkpoint_path, file_results)
    #
    #                         except Exception as e:
    #                             logger.error(f"A task failed unexpectedly: {e}")
    #
    #     finally:
    #         # Gracefully stop the Heartbeat and Rate Monitor Threads
    #         logger.info("Download process finished. Stopping background threads...")
    #         self._keep_alive_active.set()
    #         # REMOVED: The join() call is unnecessary for a daemon thread and causes a deadlock.
    #         # if self._keep_alive_thread:
    #         #     self._keep_alive_thread.join(timeout=5)
    #
    #         if rate_monitor:
    #             self._monitoring_active = False
    #             time.sleep(0.6)  # Give monitor thread time to close
    #
    #         pbar.close()
    #         # Save final checkpoint
    #         self._save_checkpoint(checkpoint_path, file_results)
    #         logger.info(
    #             f"Final checkpoint saved. {processed_count} files processed in this session."
    #         )
    #
    def _attempt_download(self, task: FileDownloadTask) -> Optional[Path]:
        """Performs a single download attempt and validates the result."""
        local_path = task.save_folder / task.filename
        try:
            with RateLimitedContext():
                self.client.historic.download_file(
                    file_path=task.file_path,
                    store_directory=str(task.save_folder),
                )

            if self._validate_downloaded_file(local_path):
                return local_path
            else:
                logger.warning(
                    f"Validation failed for {task.filename}. Deleting invalid file."
                )
                local_path.unlink(missing_ok=True)
                return None
        except Exception as e:
            # Log specific auth errors differently from generic ones
            if "API-NG_ERR_0001" in str(e) or "INVALID_SESSION_INFORMATION" in str(e):
                logger.warning(
                    f"Session error during download for {task.filename}: {e}"
                )
            else:
                logger.debug(f"Download attempt failed for {task.filename}: {e}")
            return None

    def download_single_file_task(self, task: FileDownloadTask) -> Dict[str, Any]:
        """
        Public-facing worker method. Checks for existing files, calls the
        retry logic, and returns a structured result dictionary.
        """
        local_path = task.save_folder / task.filename

        if local_path.exists():
            if self._validate_downloaded_file(local_path):
                return {
                    "status": DownloadStatus.SKIPPED,
                    "reason": "Already exists and valid",
                    "file": task.filename,
                    "sofa_id": task.sofa_id,
                }
            else:
                local_path.unlink(missing_ok=True)

        try:
            downloaded_path = self._download_with_retries(task)
            return {
                "status": DownloadStatus.SUCCESS,
                "file": task.filename,
                "sofa_id": task.sofa_id,
                "size": downloaded_path.stat().st_size,
            }
        except Exception as e:
            # Check if the error indicates an invalid HTML file was downloaded
            error_str = str(e)
            if "validation" in error_str.lower():
                status = DownloadStatus.INVALID
            else:
                status = DownloadStatus.FAILED

            return {
                "status": status,
                "error": error_str,
                "file": task.filename,
                "sofa_id": task.sofa_id,
            }

    # =========================================================================
    # Helper Methods (File Validation, Checkpoints, Summaries)
    # =========================================================================

    def _validate_downloaded_file(self, file_path: Path) -> bool:
        """Validates that a downloaded file is compressed JSON, not an HTML error page."""
        error_file_size = self.cfg.betfair_downloader.get("error_file_size", 1024)
        if not file_path.exists() or file_path.stat().st_size < error_file_size:
            return False

        try:
            with bz2.open(file_path, "rt", encoding="utf-8") as f:
                first_line = f.readline().strip()
                # Check for common HTML/XML markers
                if any(
                    marker in first_line.lower()
                    for marker in ["<!doctype", "<html", "<?xml"]
                ):
                    return False
                # Ensure it's valid JSON
                json.loads(first_line)
                return True
        except (IOError, UnicodeDecodeError, json.JSONDecodeError, bz2.BZ2Error):
            return False

    def download_from_phase1_json(
        self, json_path: Path, output_dir: Path, resume: bool = True
    ) -> Dict[str, Any]:
        """Main entry point to start a bulk download job from a phase 1 JSON file."""
        self._reset_session_stats()
        logger.info(f"Loading Phase 1 results from: {json_path}")
        with open(json_path, "r") as f:
            phase1_data = json.load(f)

        matches = {
            k: v
            for k, v in phase1_data.get("matches", {}).items()
            if v.get("status") == "complete"
        }
        logger.info(f"Found {len(matches)} matches with status 'complete'.")

        if not matches:
            logger.warning("No complete matches to download.")
            return {"status": "no_matches", "summary": self.session_stats}

        output_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_path = output_dir / "download_checkpoint.json"
        checkpoint = self._load_checkpoint(checkpoint_path) if resume else {}

        download_tasks = self._build_download_tasks(matches, output_dir, checkpoint)
        self.session_stats["total_files_in_job"] = len(download_tasks)

        self._initialize_match_results(matches, checkpoint)
        self._process_file_downloads(download_tasks, checkpoint_path)

        return self._generate_final_summary(output_dir)

    def _build_download_tasks(
        self, matches: Dict, output_dir: Path, checkpoint: Dict
    ) -> List[FileDownloadTask]:
        """Builds a list of file download tasks, skipping already completed files."""
        tasks = []
        processed_files = {
            fp
            for fp, res in checkpoint.get("file_results", {}).items()
            if res["status"] == DownloadStatus.SUCCESS
        }

        for sofa_id, match_data in matches.items():
            betfair_id = str(match_data.get("betfair_match_id"))
            if not betfair_id or betfair_id == "None":
                continue

            match_dir = output_dir / betfair_id
            match_dir.mkdir(exist_ok=True)

            for file_path in match_data.get("market_files", []):
                if file_path not in processed_files:
                    tasks.append(
                        FileDownloadTask(
                            sofa_id=sofa_id,
                            betfair_id=betfair_id,
                            file_path=file_path,
                            save_folder=match_dir,
                        )
                    )
        logger.info(
            f"Built {len(tasks)} new download tasks. {len(processed_files)} files already complete."
        )
        return tasks

    def _save_checkpoint(self, checkpoint_path: Path, file_results: Dict):
        """Saves download progress to a checkpoint file."""
        with self.results_lock:
            checkpoint = {
                "last_updated": datetime.now().isoformat(),
                "file_results": file_results,
                "match_results": self.match_results,
            }
        try:
            with open(checkpoint_path, "w") as f:
                json.dump(checkpoint, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self, checkpoint_path: Path) -> Dict:
        """Loads download progress from a checkpoint file."""
        if not checkpoint_path.exists():
            return {}
        try:
            with open(checkpoint_path, "r") as f:
                checkpoint = json.load(f)
            logger.info(f"Resuming from checkpoint: {checkpoint_path}")
            return checkpoint
        except (json.JSONDecodeError, IOError) as e:
            logger.error(
                f"Could not load or parse checkpoint file. Starting fresh. Error: {e}"
            )
            return {}

    def _initialize_match_results(self, matches: Dict, checkpoint: Dict):
        """Initializes the tracking dictionary for match-level statistics."""
        self.match_results = checkpoint.get("match_results", {})
        for sofa_id, data in matches.items():
            if sofa_id not in self.match_results:
                self.match_results[sofa_id] = {
                    "betfair_id": data.get("betfair_match_id"),
                    "total_files": len(data.get("market_files", [])),
                    "downloaded": 0,
                    "failed": 0,
                    "invalid": 0,
                    "skipped": 0,
                    "status": DownloadStatus.FAILED,
                }

    def _update_match_result(self, task: FileDownloadTask, result: Dict):
        """Updates the status of a single match based on a file download result."""
        with self.results_lock:
            match = self.match_results.get(task.sofa_id)
            if not match:
                return

            status_map = {
                DownloadStatus.SUCCESS: "downloaded",
                DownloadStatus.FAILED: "failed",
                DownloadStatus.INVALID: "invalid",
                DownloadStatus.SKIPPED: "skipped",
            }
            status_key = status_map.get(result["status"])
            if status_key:
                match[status_key] += 1

            successful_downloads = match["downloaded"] + match["skipped"]
            if successful_downloads == match["total_files"]:
                match["status"] = DownloadStatus.SUCCESS
            elif successful_downloads > 0:
                match["status"] = DownloadStatus.PARTIAL
            else:
                match["status"] = DownloadStatus.FAILED

    def _update_session_stats(self, result: Dict):
        """Updates the overall session statistics."""
        with self.results_lock:
            self.session_stats["processed_files_in_session"] += 1
            if result["status"] == DownloadStatus.SUCCESS:
                self.session_stats["downloaded_files"] += 1
            elif result["status"] == DownloadStatus.FAILED:
                self.session_stats["failed_files"] += 1
            elif result["status"] == DownloadStatus.INVALID:
                self.session_stats["invalid_files"] += 1
            elif result["status"] == DownloadStatus.SKIPPED:
                self.session_stats["skipped_files"] += 1

    def _generate_final_summary(self, output_dir: Path) -> Dict:
        """Generates and saves a final summary of the entire download job."""
        summary = {
            "session_start": self.session_stats["session_start"],
            "session_end": datetime.now().isoformat(),
            "file_stats": self.session_stats,
            "match_stats": {
                "total_matches": len(self.match_results),
                "successful": sum(
                    1
                    for m in self.match_results.values()
                    if m["status"] == DownloadStatus.SUCCESS
                ),
                "partial": sum(
                    1
                    for m in self.match_results.values()
                    if m["status"] == DownloadStatus.PARTIAL
                ),
                "failed": sum(
                    1
                    for m in self.match_results.values()
                    if m["status"] == DownloadStatus.FAILED
                ),
            },
            "match_details": self.match_results,
        }
        summary_path = output_dir / "download_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Final summary saved to: {summary_path}")
        return summary
