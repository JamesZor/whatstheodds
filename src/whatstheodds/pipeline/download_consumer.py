# src/whatstheodds/pipeline/download_consumer.py
import bz2
import json
import logging
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from whatstheodds.betfair.dataclasses import BetfairSearchSingleMarketResult
from whatstheodds.betfair.downloader import BetfairDownloader
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager
from whatstheodds.extractors.processor_factory import ProcessorFactory

logger = logging.getLogger(__name__)


class DownloadConsumer:
    def __init__(self, batch_size: int = 10, max_workers: int = 2):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.downloader = BetfairDownloader(cfg=self.config)
        self.factory = ProcessorFactory(cfg=self.config)

        self.batch_size = batch_size
        self.max_workers = max_workers

    def _download_and_extract(
        self, market_id: str, match_id: int, market_type: str, file_path: str
    ) -> dict:
        ticks_to_insert = []

        try:
            # 1. Fetch the correct Extractor for this market
            processor = self.factory.get_processor(market_type)
            if not processor:
                raise ValueError(
                    f"No processor configured in Factory for {market_type}"
                )

            # 2. Create the mock result for your Downloader
            mock_result = BetfairSearchSingleMarketResult(
                strategy_used="QUEUE_DOWNLOAD",
                market_type=market_type,
                file=file_path,
                match_id=str(match_id),
                market_id=market_id,
            )

            # 3. Spin up the auto-deleting temp directory
            with tempfile.TemporaryDirectory() as tmp_dir:
                save_dir = Path(tmp_dir)

                # 4. Download using your robust retry logic
                downloaded_file = self.downloader._download_with_retries(
                    search_result=mock_result, save_folder=save_dir, retry_on_html=True
                )

                if not downloaded_file or not downloaded_file.exists():
                    raise FileNotFoundError(
                        f"Betfair API failed to return file for market {market_id}"
                    )

                # 5. Load the entire file into memory as a list of dicts
                raw_file_data = []
                with bz2.BZ2File(downloaded_file, "rb") as decompressed_file:
                    for line in decompressed_file:
                        if line:
                            raw_file_data.append(json.loads(line))

                # 6. Pass data through the Extractor Factory!
                clean_odds_dict = processor.process(raw_file_data)

                # 7. Format the single row for PostgreSQL
                ticks_to_insert.append(
                    {
                        "match_id": match_id,
                        "market_id": str(market_id),
                        "odds_data": clean_odds_dict,
                    }
                )

            # The 'with' block ends here, instantly deleting tmp_dir and the .bz2 file!
            return {
                "market_id": market_id,
                "status": "SUCCESS",
                "error": None,
                "ticks": ticks_to_insert,
            }

        except Exception as e:
            logger.error(f"Failed to process market {market_id}: {e}")
            return {
                "market_id": market_id,
                "status": "FAILED",
                "error": str(e),
                "ticks": [],
            }

    def run(self):
        logger.info(
            f"Starting Download Consumer. Batch size: {self.batch_size}, Workers: {self.max_workers}"
        )

        while True:
            # 1. Ask Postgres for a batch of jobs (using FOR UPDATE SKIP LOCKED)
            markets = self.db.claim_pending_markets(limit=self.batch_size)

            if not markets:
                logger.info("Queue is empty. Sleeping for 30 seconds...")
                time.sleep(30)
                continue

            logger.info(f"Claimed {len(markets)} markets. Beginning downloads...")

            # 2. Process the batch concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._download_and_extract, m_id, match_id, m_type, f_path
                    )
                    for m_id, match_id, m_type, f_path in markets
                ]
                results = [f.result() for f in futures]

            # 3. Gather results and update the Database
            all_ticks = []
            success_count = 0

            for res in results:
                all_ticks.extend(res["ticks"])

                # Update the status in the markets queue
                self.db.update_market_status(
                    market_id=str(res["market_id"]),
                    status=res["status"],
                    error_log=res["error"],
                )

                if res["status"] == "SUCCESS":
                    success_count += 1

            # 4. Perform the massive bulk insert of all ticks
            if all_ticks:
                self.db.bulk_insert_odds(all_ticks)

            logger.info(
                f"Batch complete. Downloaded {success_count}/{len(markets)}. Inserted {len(all_ticks)} fully processed markets into Postgres."
            )


#############

if __name__ == "__main__":
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
markets
# [('242633067', 12473328, 'FIRST_HALF_GOALS_05', '/xds_nfs/edp_processed/BASIC/2025/Apr/26/34236342/1.242633067.bz2')]

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

    ### Step 2

# 1. Create the mock result expected by your Downloader class
mock_result = BetfairSearchSingleMarketResult(
    strategy_used="REPL_TEST",
    market_type=market_type,  # Using the market_type from Step 1
    file=file_path,  # Using the file_path from Step 1
    match_id=match_id,  # Using the match_id from Step 1
    market_id=market_id,  # Using the market_id from Step 1
)

import bz2
import tempfile
from pathlib import Path  # <-- Make sure this is imported

with tempfile.TemporaryDirectory() as tmp_dir:
    # 1. Convert the string to a Path object immediately!
    save_dir = Path(tmp_dir)
    print(f"Created temporary holding folder: {save_dir}")

    # 2. Trigger the download, passing our Path object
    downloaded_file = downloader._download_with_retries(
        search_result=mock_result, save_folder=save_dir, retry_on_html=True
    )

    print(f"\n--- STEP 2 COMPLETE ---")
    print(f"Is file valid?: {downloaded_file is not None}")

    if downloaded_file:
        print(f"Saved temporarily to: {downloaded_file}")
        print(f"File size: {downloaded_file.stat().st_size} bytes")

        # We can peek at the first line right here before the folder deletes!
        with bz2.BZ2File(downloaded_file, "rb") as f:
            first_line = f.readline()
            print("\nFirst line of data extracted successfully:")
            print(first_line[:150], "...\n")

# The moment we exit the 'with' block, tmp_dir and the file vanish!
print("Temporary folder auto-deleted.")
# Created temporary holding folder: /tmp/tmpb9x2o57t
#
# --- STEP 2 COMPLETE ---
# Is file valid?: True
# Saved temporarily to: /tmp/tmpb9x2o57t/1.242633071.bz2
# File size: 1085 bytes
#
# First line of data extracted successfully:
# b'{"op":"mcm","clk":"13706119847","pt":1745503249500,"mc":[{"id":"1.242633071","marketDefinition":{"bspMarket":false,"turnInPlayEnabled":true,"persisten' ...
#
# Temporary folder auto-deleted.


import bz2
import json
from pprint import pprint

from whatstheodds.extractors.processor_factory import ProcessorFactory

# 1. Initialize your factory
factory = ProcessorFactory()
processor = factory.get_processor(market_type)


with tempfile.TemporaryDirectory() as tmp_dir:
    # 1. Convert the string to a Path object immediately!
    save_dir = Path(tmp_dir)
    print(f"Created temporary holding folder: {save_dir}")

    # 2. Trigger the download, passing our Path object
    downloaded_file = downloader._download_with_retries(
        search_result=mock_result, save_folder=save_dir, retry_on_html=True
    )

    print(f"\n--- STEP 2 COMPLETE ---")
    print(f"Is file valid?: {downloaded_file is not None}")

    if downloaded_file:
        print(f"Saved temporarily to: {downloaded_file}")
        print(f"File size: {downloaded_file.stat().st_size} bytes")

        raw_file_data = []
        with bz2.BZ2File(downloaded_file, "rb") as f:
            for line in f:
                if line:
                    raw_file_data.append(json.loads(line))

            print(f"Loaded {len(raw_file_data)} raw Betfair updates.")

            # 3. Pass the data through your Extractor!
            print("Processing data through Extractor...")
            clean_odds_dict = processor.process(raw_file_data)

            # 4. Format it for PostgreSQL
            # Because clean_odds_dict contains ALL the history, we only insert ONE row!
            ticks_to_insert = [
                {
                    "match_id": match_id,
                    "market_id": str(market_id),
                    "odds_data": clean_odds_dict,
                }
            ]

            print(f"\n--- STEP 4 COMPLETE ---")
            print(f"Ready to insert 1 row into odds_history.")
            print("Preview of the cleaned odds_data (first 3 timestamps):")

            # Let's peek at the first 3 items in the lists to verify it worked
            preview = {
                "timestamps": clean_odds_dict["timestamps"][:3],
                "home": clean_odds_dict["home"][:3],
                "away": clean_odds_dict["away"][:3],
                "draw": (
                    clean_odds_dict["draw"][:3] if "draw" in clean_odds_dict else None
                ),
            }
            print(preview)


from pprint import pprint

print("\n--- STEP 4 COMPLETE ---")
print(f"Ready to insert 1 row into odds_history.")
print(f"The Extractor generated these keys: {list(clean_odds_dict.keys())}")

print("\nPreview of the cleaned odds_data (first 3 items of each list):")
# Dynamically grab the first 3 items of whatever lists are in the dictionary
preview = {}
for key, value in clean_odds_dict.items():
    if isinstance(value, list):
        preview[key] = value[:3]
    else:
        preview[key] = value

pprint(preview)
