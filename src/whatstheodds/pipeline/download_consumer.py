import bz2
import json
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.betfair.rate_limiter import RateLimitedContext
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager

logger = logging.getLogger(__name__)


class DownloadConsumer:
    def __init__(self, batch_size: int = 10, max_workers: int = 2):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.client = setup_betfair_api_client()

        self.batch_size = batch_size
        self.max_workers = max_workers

        # We REMOVED self.client.login()!
        # setup_betfair_api_client() already handles the correct auth for historical data.

    def _download_and_extract(
        self, market_id: str, match_id: int, market_type: str
    ) -> dict:
        """Worker function: Downloads to temp folder, extracts, and auto-cleans up."""
        ticks_to_insert = []

        try:
            # 1. Create an auto-deleting temporary directory
            with tempfile.TemporaryDirectory() as tmp_dir:

                # 2. Wrap the betfairlightweight call in your Rate Limiter
                with RateLimitedContext():
                    # Let the library handle the complex downloading logic
                    file_path = self.client.historic.download_file(
                        file_path=tmp_dir, file_id=market_id
                    )

                # Ensure the file was actually saved
                if not file_path or not os.path.exists(file_path):
                    raise FileNotFoundError(
                        f"Betfair API failed to save file for market {market_id}"
                    )

                # 3. Read and extract the downloaded .bz2 file
                with bz2.BZ2File(file_path, "rb") as decompressed_file:
                    for line in decompressed_file:
                        tick_data = json.loads(line)

                        # --- YOUR EXTRACTOR LOGIC CAN GO HERE ---
                        # For now, we save the raw JSON directly to Postgres
                        ticks_to_insert.append(
                            {
                                "match_id": match_id,
                                "market_id": str(market_id),
                                "odds_data": tick_data,
                            }
                        )

            # The moment the code exits the 'with tempfile.TemporaryDirectory()' block,
            # Python completely deletes tmp_dir and the .bz2 file from your disk!

            return {
                "market_id": market_id,
                "status": "SUCCESS",
                "error": None,
                "ticks": ticks_to_insert,
            }

        except Exception as e:
            logger.error(f"Failed market {market_id}: {str(e)}")
            return {
                "market_id": market_id,
                "status": "FAILED",
                "error": str(e),
                "ticks": [],
            }

    def run(self):
        logger.info(f"Starting Download Consumer. Batch size: {self.batch_size}")

        while True:
            markets = self.db.claim_pending_markets(limit=self.batch_size)

            if not markets:
                logger.info("Queue is empty. Sleeping for 30 seconds...")
                time.sleep(30)
                continue

            logger.info(
                f"Claimed {len(markets)} markets. Downloading to Temp Storage..."
            )

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._download_and_extract, m_id, match_id, m_type)
                    for m_id, match_id, m_type in markets
                ]
                results = [f.result() for f in futures]

            all_ticks = []
            success_count = 0

            for res in results:
                all_ticks.extend(res["ticks"])

                self.db.update_market_status(
                    market_id=str(res["market_id"]),
                    status=res["status"],
                    error_log=res["error"],
                )

                if res["status"] == "SUCCESS":
                    success_count += 1

            if all_ticks:
                self.db.bulk_insert_odds(all_ticks)

            logger.info(
                f"Batch complete. Downloaded {success_count}/{len(markets)} successfully. Inserted {len(all_ticks)} ticks into DB."
            )


if __name__ == "__main__":
    # Start with max_workers=2 to be kind to Betfair's rate limits
    consumer = DownloadConsumer(batch_size=10, max_workers=2)
    consumer.run()


import bz2
import json
import os
from pathlib import Path
from pprint import pprint

from whatstheodds.betfair.dataclasses import BetfairSearchSingleMarketResult
from whatstheodds.betfair.downloader import BetfairDownloader
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager

# 1. Initialize our core tools
config = load_config()
db = DatabaseManager(config)
downloader = BetfairDownloader()

# 2. Claim exactly ONE market from the database queue
# (This immediately changes its status to 'DOWNLOADING' in Postgres)
markets = db.claim_pending_markets(limit=1)

if not markets:
    print("Queue is empty! Run the SearchProducer first.")
else:
    # Unpack our single market
    market_id, match_id, market_type, file_path = markets[0]

    print(f"--- STEP 1 COMPLETE ---")
    print(f"Match ID:   {match_id}")
    print(f"Market ID:  {market_id}")
    print(f"Type:       {market_type}")
    print(f"File Path:  {file_path}")
