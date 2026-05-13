# scripts/ireland_test_grab.py
# import logging
import sys

from whatstheodds.pipeline.download_consumer import DownloadConsumer
from whatstheodds.pipeline.search_producer import SearchProducer

# Set up basic logging so you can see the terminal output clearly
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# )
# # logger = logging.getLogger("IrelandGrabber")
#


def main():
    # --- TARGET CONFIGURATION ---
    # Ireland Premier Division (2025)
    prem_tourn_id = 79
    prem_season_id = 69981

    # Ireland First Division (2025)
    div1_tourn_id = 718
    div1_season_id = 57305

    target_tournaments = [prem_tourn_id, div1_tourn_id]
    target_seasons = [prem_season_id, div1_season_id]

    logger.info(
        f"Initiating Betfair Extraction Pipeline for Tournaments: {target_tournaments}"
    )

    # ==========================================
    # PHASE 1: THE SEARCH PRODUCER
    # Maps Sofascore matches to Betfair files
    # ==========================================
    logger.info("=== Starting Phase 1: Search Producer ===")
    producer = SearchProducer()

    # Run the producer to fill up the betfair.markets queue
    producer.run(
        tournament_filters=target_tournaments,
        season_filters=target_seasons,
        # limit_filter=10  # Optional: Un-comment to test a small batch first
    )
    logger.info("Phase 1 Complete. Target matches have been successfully queued.")

    # ==========================================
    # PHASE 2: THE DOWNLOAD CONSUMER
    # Drains the queue, downloads, stitches, and extracts
    # ==========================================
    logger.info("=== Starting Phase 2: Download Consumer ===")
    # 5 workers is usually the sweet spot for the Betfair Historical API rate limit
    consumer = DownloadConsumer(batch_size=10, max_workers=5, show_rate_limit_bar=True)

    # Optional: Throw any old network failures back into the PENDING queue before we start
    consumer.retry_failed_markets(max_retries=3)

    # Run until the queue is completely empty (continuous=False)
    consumer.run()

    logger.info(
        "Phase 2 Complete. All available Ireland odds have been saved to the database."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Exiting safely.")
        sys.exit(0)
