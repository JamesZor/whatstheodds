# src/whatstheodds/pipeline/download_consumer.py


import bz2
import json
import logging
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
        self.show_rate_limit_bar = show_rate_limit_bar
        self._monitoring_active = False

    def retry_failed_markets(self, max_retries: int = 3):
        """Pushes failed markets back into the queue for another attempt."""
        count = self.db.requeue_failed_markets(max_retries=max_retries)
        logger.info(f"Re-queued {count} failed markets for retry.")
        return count

    def _create_rate_limit_monitor(self, position: int = 1) -> Optional[tqdm]:
        """Creates a separate progress bar for rate limit monitoring in a background thread."""
        if not self.show_rate_limit_bar:
            return None

        rate_bar = tqdm(
            total=100,
            desc="Rate Limit",
            position=position,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {postfix}",
            leave=False,
            colour="yellow",
        )

        def update_rate_bar():
            while self._monitoring_active:
                try:
                    status = betfair_rate_limiter.get_current_usage()
                    rate_bar.n = int(status["usage_percent"])

                    if status["usage_percent"] > 90:
                        rate_bar.colour = "red"
                    elif status["usage_percent"] > 70:
                        rate_bar.colour = "yellow"
                    else:
                        rate_bar.colour = "green"

                    rate_bar.set_postfix(
                        {
                            "reqs": f"{status['current_requests']}/{status['max_requests']}",
                            "window": f"{status['time_window']}s",
                        }
                    )
                    rate_bar.refresh()
                except Exception:
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

            mock_result = BetfairSearchSingleMarketResult(
                strategy_used="QUEUE_DOWNLOAD",
                market_type=market_type,
                file=file_path,
                match_id=str(match_id),
                market_id=market_id,
            )

            with tempfile.TemporaryDirectory() as tmp_dir:
                save_dir = Path(tmp_dir)
                downloaded_file = self.downloader._download_with_retries(
                    search_result=mock_result, save_folder=save_dir, retry_on_html=True
                )

                if not downloaded_file or not downloaded_file.exists():
                    raise FileNotFoundError("API failed to return valid file")

                raw_file_data = []
                with bz2.BZ2File(downloaded_file, "rb") as f:
                    for line in f:
                        if line:
                            raw_file_data.append(json.loads(line))

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

    def run(self, max_batches: Optional[int] = None, continuous: bool = False):
        """
        Runs the consumer.
        - max_batches: Stop after N batches (leave None for unlimited).
        - continuous: If True, sleep when queue is empty. If False, exit when queue is empty.
        """
        logger.info(
            f"Starting Download Consumer (Batch: {self.batch_size}, Workers: {self.max_workers})"
        )
        batches_processed = 0
        rate_monitor = self._create_rate_limit_monitor(position=1)

        try:
            while True:
                if max_batches and batches_processed >= max_batches:
                    logger.info(f"Reached batch limit ({max_batches}). Stopping.")
                    break

                markets = self.db.claim_pending_markets(limit=self.batch_size)

                if not markets:
                    if continuous:
                        time.sleep(30)
                        continue
                    else:
                        logger.info("Queue is empty. Processing complete.")
                        break

                all_ticks = []
                success_count = 0

                # Process batch with concurrent futures and a progress bar
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._download_and_extract, m_id, match_id, m_type, f_path
                        ): m_id
                        for m_id, match_id, m_type, f_path in markets
                    }

                    with tqdm(
                        total=len(futures),
                        desc=f"Batch {batches_processed + 1}",
                        position=0,
                        leave=True,
                    ) as pbar:
                        for future in as_completed(futures):
                            res = future.result()

                            # DB Updates
                            self.db.update_market_status(
                                str(res["market_id"]), res["status"], res["error"]
                            )
                            all_ticks.extend(res["ticks"])

                            if res["status"] == "SUCCESS":
                                success_count += 1
                                pbar.set_postfix(status="✓")
                            else:
                                pbar.set_postfix(status="✗")
                            pbar.update(1)

                # Massive bulk insert
                if all_ticks:
                    self.db.bulk_insert_odds(all_ticks)

                batches_processed += 1

        finally:
            # Clean up the rate limit monitoring thread safely
            if rate_monitor:
                self._monitoring_active = False
                time.sleep(0.5)


if __name__ == "__main__":
    consumer = DownloadConsumer(batch_size=10, max_workers=2)

    # Example workflow:
    # 1. Retry anything that failed previously (up to 3 times)
    consumer.retry_failed_markets(max_retries=3)

    # 2. Run until the queue is completely empty
    consumer.run(continuous=False)
