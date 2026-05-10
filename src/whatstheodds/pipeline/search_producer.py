# src/whatstheodds/pipeline/search_producer.py
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from whatstheodds.betfair.dataclasses import BetfairSearchRequest
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager
from whatstheodds.mappers.match_mapper import match_mapping

logger = logging.getLogger(__name__)


class SearchProducer:
    def __init__(
        self, tournament_filters: Optional[List[int]] = None, max_workers: int = 5
    ):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.search_engine = BetfairSearchEngine(cfg=self.config)
        self.tournament_filters = tournament_filters
        self.max_workers = max_workers

    def process_match(self, match_data):
        """Worker function for a single match."""
        match_id, home, away, start_time, country = match_data

        try:
            # 1. Map Names
            mapped_home, mapped_away = match_mapping(home, away)

            # 2. Search Betfair
            request = BetfairSearchRequest(
                sofa_match_id=match_id,
                home=mapped_home,
                away=mapped_away,
                date=start_time,
                country=country or "GB",
            )

            result = self.search_engine.search_main(request)

            if result.match_id:
                # --- SUCCESS PATH ---
                strategy = (
                    list(result.valid_markets.values())[0].strategy_used
                    if result.valid_markets
                    else "unknown"
                )

                self.db.upsert_betfair_meta(
                    match_id=match_id,
                    betfair_id=str(result.match_id),
                    name=f"{result.home} v {result.away}",
                    strategy=strategy,
                )

                markets_to_queue = [
                    (str(m.market_id), m.market_type)
                    for m in result.valid_markets.values()
                ]
                self.db.queue_markets(match_id, markets_to_queue)

                return f"SUCCESS: {home} v {away} ({len(markets_to_queue)} markets)"

            else:
                # --- FAILURE PATH ---
                # We log it so we don't keep searching for it, allowing manual investigation
                self.db.upsert_betfair_meta(
                    match_id=match_id,
                    betfair_id=None,
                    # Fallback to mapped names for logging since result.home might be None
                    name=f"{mapped_home} v {mapped_away}",
                    strategy="FAILED_SEARCH",
                )

                return f"SKIPPED/LOGGED: {home} v {away} (Not found on Betfair)"

        except Exception as e:
            logger.error(f"Failed to process match {match_id}: {e}")
            return f"ERROR: {match_id} - {str(e)}"

    def run(self):
        matches = self.db.get_unprocessed_matches(
            tournament_ids=self.tournament_filters
        )
        logger.info(
            f"Retrieved {len(matches)} matches. Starting thread pool with {self.max_workers} workers."
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Map the worker function over our list of matches
            results = list(executor.map(self.process_match, matches))

        # Summary of results
        for res in results:
            logger.debug(res)

        logger.info("Search Producer run complete.")


if __name__ == "__main__":
    # You can now specify how many threads to run (e.g., 5 to 10 is usually safe for search)
    producer = SearchProducer(tournament_filters=[1, 2], max_workers=5)
    producer.run()
