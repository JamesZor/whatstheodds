# src/whatstheodds/pipeline/search_producer.py
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
        self, match_id: int, request: BetfairSearchResult, result: BetfairSearchResult
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
            kickoff_time=request.date,
            strategy=strategy,
        )

        markets_to_queue = [
            (str(m.market_id), m.market_type) for m in result.valid_markets.values()
        ]
        self.db.queue_markets(match_id, markets_to_queue)

        pass

    def _fail_process_match(
        self, match_id: int, request: BetfairSearchRequest, result: BetfairSearchResult
    ):
        # If the match failed, valid_markets is empty.
        # We grab the strategy tracker from the first missing market instead.
        strategy = (
            list(result.missing_markets.values())[0].strategy_used + "_FAILED"
            if result.missing_markets
            else "FAILED_UNKNOWN"
        )

        # We log it so we don't keep searching for it, allowing manual investigation
        self.db.upsert_betfair_meta(
            match_id=match_id,
            betfair_id=None,  # Now allowed because of nullable=True
            name=f"{request.home} v {request.away}",
            kickoff_time=request.date,
            strategy=strategy,
        )

    def process_match(self, match_data):
        """Worker function for a single match."""
        # Note: Ensure self.db and self.mapper are initialized in __init__
        match_id, home, away, start_time, country = match_data

        try:
            # 1. Convert timestamp to datetime (as we discussed)
            kickoff_dt = datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)

            # 2. Map Names (Where the 400 often starts if mapping fails)
            request = self.mapper.map_match(
                sofa_match_id=match_id,
                home_team_slug=home,
                away_team_slug=away,
                date=kickoff_dt,
            )

            # 3. Execute Search (Primary + Extended fallback)
            result = self.search_engine.search_main(request)

            if result.match_id:
                # SUCCESS: Update DB to 'COMPLETED' or 'VERIFIED'
                self._success_process_match(match_id, request, result)
            else:
                # STRATEGY FAILURE: No match found on Betfair
                # Log as 'FAILED_NOT_FOUND' so you don't keep wasting API calls
                self._fail_process_match(
                    match_id, "No match found with current strategies"
                )

        except betfairlightweight.exceptions.APIError as e:
            # API FAILURE: (e.g., the 400 error)
            # Log as 'RETRY_REQUIRED'
            error_msg = f"Betfair API 400/500: {str(e)}"
            logger.error(f"Match {match_id} API Error: {error_msg}")
            self.db.update_match_status(match_id, status="RETRY", error_log=error_msg)

        except Exception as e:
            # GENERAL FAILURE: (e.g., Database connection or Logic error)
            error_msg = f"Critical Failure: {str(e)}"
            logger.exception(f"Unexpected error on match {match_id}")
            self.db.update_match_status(match_id, status="ERROR", error_log=error_msg)

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
    producer.run(tournament_filters=[54], limit_filter=5)
