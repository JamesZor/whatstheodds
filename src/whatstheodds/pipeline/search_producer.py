# src/whatstheodds/pipeline/search_producer.py
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import List, Optional

import betfairlightweight  # type: ignore

from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.configs.config import load_config
from whatstheodds.db.manager import DatabaseManager
from whatstheodds.mappers.match_mapper import MatchMapper

logger = logging.getLogger(__name__)


class SearchProducer:
    def __init__(self):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.search_engine = BetfairSearchEngine(cfg=self.config)
        self.mapper = MatchMapper()

    def _succes_process_match(
        self, match_id: int, result: BetfairSearchResult, start_time: datetime
    ):
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
            kickoff_time=start_time,
            status="SUCCESS",
        )

        markets_to_queue = [
            (str(m.market_id), m.market_type, m.file)
            for m in result.valid_markets.values()
        ]
        self.db.queue_markets(match_id, markets_to_queue)

    def _fail_process_match(
        self, match_id: int, request: BetfairSearchRequest, result: BetfairSearchResult
    ):
        strategy = (
            list(result.missing_markets.values())[0].strategy_used + "_FAILED"
            if result.missing_markets
            else "FAILED_UNKNOWN"
        )

        self.db.upsert_betfair_meta(
            match_id=match_id,
            betfair_id=None,
            name=f"{request.home} v {request.away}",
            strategy=strategy,
            kickoff_time=request.date,
            status="NOT_FOUND",  # It didn't crash, it just isn't there
            error_log="Match not found in Betfair historical data",
        )

    def process_match(self, match_data):
        """Worker function for a single match."""
        match_id, home, away, start_time, country = match_data

        try:
            # 1. Convert timestamp to datetime
            kickoff_dt = datetime.fromtimestamp(start_time, tz=timezone.utc)

            # HACK:
            # 2. Map Names
            # (Note: Let's force "GB" here just to test if the country code was the issue!)
            safe_country = (
                "GB"
                if country in ["England", "Scotland", "United Kingdom"]
                else (country or "GB")
            )

            request = self.mapper.map_match(
                sofa_match_id=match_id,
                home_team_slug=home,
                away_team_slug=away,
                date=kickoff_dt,
                country_code=safe_country,
            )

            # --- THE MAPPING SAFETY NET ---
            if request is None:
                self.db.upsert_betfair_meta(
                    match_id=match_id,
                    betfair_id=None,
                    name=f"{home} v {away}",
                    strategy="MAPPING_FAILED",
                    kickoff_time=kickoff_dt,
                    status="FAILED",
                    error_log="Team mapping failed (missing in JSON)",
                )
                return f"MAPPING FAILED: {home} v {away}"

            # 3. Search Betfair
            result = self.search_engine.search_main(request)

            if result.match_id:
                self._succes_process_match(match_id, result, kickoff_dt)
                return f"SUCCESS: {home} v {away}"
            else:
                self._fail_process_match(match_id, request, result)
                return f"NOT FOUND: {home} v {away}"

        except Exception as e:
            logger.error(f"Failed to process match {match_id}: {e}")

            # --- THE API CRASH SAFETY NET ---
            self.db.upsert_betfair_meta(
                match_id=match_id,
                betfair_id=None,
                name=f"{home} v {away}",
                strategy="API_CRASH",
                kickoff_time=datetime.fromtimestamp(start_time) if start_time else None,
                status="FAILED",
                error_log=str(e),
            )
            return f"ERROR: {match_id} - {str(e)}"

    def run(
        self,
        tournament_filters: Optional[List[int]] = None,
        season_filters: Optional[List[int]] = None,
        limit_filter: Optional[int] = None,
    ):
        matches = self.db.get_unprocessed_matches(
            tournament_ids=tournament_filters,
            season_ids=season_filters,
            limit=limit_filter,
        )
        logger.info(
            f"Retrieved {len(matches)} matches. Starting thread pool with {self.config.max_workers.search} workers."
        )

        with ThreadPoolExecutor(max_workers=self.config.max_workers.search) as executor:
            # Map the worker function over our list of matches
            executor.map(self.process_match, matches)

        logger.info("Search Producer run complete.")


if __name__ == "__main__":
    # You can now specify how many threads to run (e.g., 5 to 10 is usually safe for search)
    producer = SearchProducer()
    producer.run(tournament_filters=[56], limit_filter=5)
