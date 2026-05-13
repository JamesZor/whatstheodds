# src/whatstheodds/pipeline/download_consumer.py

import bz2
import json
import logging
import signal
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from whatstheodds.betfair.dataclasses import BetfairSearchSingleMarketResult
from whatstheodds.betfair.downloader import BetfairDownloader
from whatstheodds.betfair.rate_limiter import betfair_rate_limiter
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager
from whatstheodds.extractors.processor_factory import ProcessorFactory

logger = logging.getLogger(__name__)


class DownloadConsumer:
    def __init__(
        self,
        batch_size: int = 10,
        max_workers: int = 5,
        show_rate_limit_bar: bool = True,
    ):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.downloader = BetfairDownloader()
        self.factory = ProcessorFactory()

        self.batch_size = batch_size
        self.max_workers = max_workers

        # State tracking
        self.show_rate_limit_bar = show_rate_limit_bar
        self._monitoring_active = False
        self._shutdown_requested = False
        self._active_market_ids = []  # Tracks what we are currently working on

        # Bind the graceful shutdown signals (Catches Ctrl+C or kill commands)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Gracefully intercepts shutdown requests to revert orphaned markets."""
        print("\n\n[!] Shutdown signal received. Cleaning up...")
        self._shutdown_requested = True
        self._monitoring_active = False

        if self._active_market_ids:
            print(
                f"Reverting {len(self._active_market_ids)} actively downloading markets to PENDING..."
            )
            self.db.revert_downloading_markets(self._active_market_ids)

        print("Cleanup complete. Exiting safely.")
        sys.exit(0)

    def retry_failed_markets(self, max_retries: int = 3):
        count = self.db.requeue_failed_markets(max_retries=max_retries)
        if count > 0:
            logger.info(f"Re-queued {count} failed markets for retry.")

    def _create_rate_limit_monitor(self, position: int = 1) -> Optional[tqdm]:
        if not self.show_rate_limit_bar:
            return None

        rate_bar = tqdm(
            total=100,
            desc="Rate Limit",
            position=position,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {postfix}",
            leave=False,
            colour="green",
        )

        def update_rate_bar():
            while self._monitoring_active:
                try:
                    status = betfair_rate_limiter.get_current_usage()

                    # USE THE CORRECT DICTIONARY KEYS:
                    usage = int(status["window_usage_percent"])
                    rate_bar.n = usage

                    rate_bar.colour = (
                        "red" if usage > 90 else "yellow" if usage > 70 else "green"
                    )

                    # Update postfix with both window limits and concurrency limits
                    rate_bar.set_postfix(
                        {
                            "reqs": f"{status['current_requests_in_window']}/{status['max_requests_in_window']}",
                            "active": f"{status['active_concurrent_downloads']}/{status['max_concurrent_downloads']}",
                        }
                    )
                    rate_bar.refresh()
                except Exception as e:
                    pass
                time.sleep(0.2)
            rate_bar.close()

        self._monitoring_active = True
        monitor_thread = threading.Thread(target=update_rate_bar, daemon=True)
        monitor_thread.start()
        return rate_bar

    def _download_and_extract(
        self, market_id: str, match_id: int, market_type: str, file_path: str
    ) -> dict:
        ticks_to_insert = []
        try:
            processor = self.factory.get_processor(market_type)
            if not processor:
                raise ValueError(f"No configured processor for {market_type}")

            raw_file_data = (
                []
            )  # We define this OUTSIDE the loop so all files append to it

            # --- THE STITCHING LOOP ---
            # Split the paths by comma (if there is only 1 file, it just loops once)
            paths_to_download = file_path.split(",")

            with tempfile.TemporaryDirectory() as tmp_dir:
                save_dir = Path(tmp_dir)

                for single_path in paths_to_download:
                    mock_result = BetfairSearchSingleMarketResult(
                        strategy_used="QUEUE_DOWNLOAD",
                        market_type=market_type,
                        file=single_path,  # Pass the individual path
                        match_id=str(match_id),
                        market_id=market_id,
                    )

                    downloaded_file = self.downloader._download_with_retries(
                        search_result=mock_result,
                        save_folder=save_dir,
                        retry_on_html=True,
                    )

                    if not downloaded_file or not downloaded_file.exists():
                        raise FileNotFoundError(
                            f"API failed to return file: {single_path}"
                        )

                    # Open the file and append the ticks to our master list
                    with bz2.BZ2File(downloaded_file, "rb") as f:
                        for line in f:
                            if line:
                                raw_file_data.append(json.loads(line))

            # --- LOOP ENDS ---

            # Now we pass the FULL, stitched match history to your Extractor!
            clean_odds_dict = processor.process(raw_file_data)

            ticks_to_insert.append(
                {
                    "match_id": match_id,
                    "market_id": str(market_id),
                    "odds_data": clean_odds_dict,
                }
            )

            return {
                "market_id": market_id,
                "status": "SUCCESS",
                "error": None,
                "ticks": ticks_to_insert,
            }

        except Exception as e:
            return {
                "market_id": market_id,
                "status": "FAILED",
                "error": str(e),
                "ticks": [],
            }

    def run(self):
        """Runs the consumer with a master progress bar."""
        total_pending = self.db.get_pending_market_count()

        if total_pending == 0:
            logger.info("Queue is completely empty. Nothing to do!")
            return

        logger.info(f"Starting Download Consumer. {total_pending} markets pending.")

        rate_monitor = self._create_rate_limit_monitor(position=1)
        batches_processed = 0

        try:
            # Create a single master progress bar (Position 0)
            with tqdm(
                total=total_pending, desc="Total Progress", position=0, leave=True
            ) as pbar:

                while not self._shutdown_requested:
                    # 1. Claim a batch
                    markets = self.db.claim_pending_markets(limit=self.batch_size)

                    if not markets:
                        break  # Queue is empty!

                    # Track the active IDs so our signal handler can revert them if we crash
                    self._active_market_ids = [m[0] for m in markets]

                    batches_processed += 1
                    pbar.set_description(f"Downloading (Batch {batches_processed})")

                    all_ticks = []

                    # 2. Process batch with concurrent futures
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = {
                            executor.submit(
                                self._download_and_extract,
                                m_id,
                                match_id,
                                m_type,
                                f_path,
                            ): m_id
                            for m_id, match_id, m_type, f_path in markets
                        }

                        for future in as_completed(futures):
                            if self._shutdown_requested:
                                break  # Stop processing new futures if user hit Ctrl+C

                            res = future.result()

                            # Update DB Status immediately for this market
                            self.db.update_market_status(
                                str(res["market_id"]), res["status"], res["error"]
                            )
                            all_ticks.extend(res["ticks"])

                            # Increment the master progress bar by 1
                            pbar.update(1)

                            # Optional: Show latest status icon in postfix
                            pbar.set_postfix(
                                status="✓" if res["status"] == "SUCCESS" else "✗"
                            )

                    if self._shutdown_requested:
                        break

                    # 3. Massive bulk insert
                    if all_ticks:
                        self.db.bulk_insert_odds(all_ticks)

                    # Clear active IDs since this batch is fully completed
                    self._active_market_ids = []

        finally:
            self._monitoring_active = False
            time.sleep(0.5)
            logger.info("Consumer run finished.")


if __name__ == "__main__":
    consumer = DownloadConsumer(batch_size=10, max_workers=2)

    # Retry any old failures first
    # consumer.retry_failed_markets(max_retries=3)

    # Drain the queue
    consumer.run()
