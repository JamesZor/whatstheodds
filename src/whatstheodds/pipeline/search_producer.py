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

COUNTRY_MAP = {
    "England": "GB",
    "Scotland": "GB",
    "Wales": "GB",
    "Northern Ireland": "GB",
    "United Kingdom": "GB",
    "Spain": "ES",
    "Italy": "IT",
    "Germany": "DE",
    "France": "FR",
    "Portugal": "PT",
    "Netherlands": "NL",
    "Belgium": "BE",
    "Brazil": "BR",
    "Argentina": "AR",
    "USA": "US",
    "Australia": "AU",
    "Turkey": "TR",
    "Greece": "GR",
    # Add any others you scrape!
}


class SearchProducer:
    def __init__(self):
        self.config = load_config()
        self.db = DatabaseManager(self.config)
        self.search_engine = BetfairSearchEngine(cfg=self.config)
        self.mapper = MatchMapper()

    def _succes_process_match(
        self, match_id: int, result: BetfairSearchResult, start_time: datetime
    ):
        # 1. Get the strategy used
        strategy = (
            list(result.valid_markets.values())[0].strategy_used
            if result.valid_markets
            else "unknown"
        )

        # 2. Extract the REAL Betfair Event ID from the first valid file path!
        # Path looks like: /.../27678990/1.122899388.bz2
        real_betfair_id = None
        first_valid = list(result.valid_markets.values())[0]
        if first_valid.file:
            # Split by comma in case it's a stitched multi-file, then grab the ID directory
            first_path = first_valid.file.split(",")[0]
            real_betfair_id = first_path.split("/")[-2]

        # 3. Compile an error log if SOME markets were found, but others were missing
        partial_errors = None
        if result.missing_markets:
            missing_names = list(result.missing_markets.keys())
            partial_errors = f"Partial Success. Missing markets: {missing_names}"

        # 4. Upsert with total transparency
        self.db.upsert_betfair_meta(
            match_id=match_id,
            betfair_id=real_betfair_id,  # <--- Now passing the REAL ID
            name=f"{result.home} v {result.away}",
            strategy=strategy,
            kickoff_time=start_time,
            status=(
                "SUCCESS" if not partial_errors else "PARTIAL_SUCCESS"
            ),  # <--- New Status!
            error_log=partial_errors,  # <--- Logs the missing markets!
        )

        # 5. Queue only the valid markets for the DownloadConsumer
        markets_to_queue = [
            (str(m.market_id), m.market_type, m.file)
            for m in result.valid_markets.values()
        ]
        self.db.queue_markets(match_id, markets_to_queue)

    def _fail_process_match(
        self, match_id: int, request: BetfairSearchRequest, result: BetfairSearchResult
    ):
        # 1. Safely extract the strategy used
        strategy = (
            list(result.missing_markets.values())[0].strategy_used
            if result.missing_markets
            else "FAILED_UNKNOWN"
        )

        # 2. Extract the specific error containing the api_params!
        error_msg = None

        # Check if there is a global error first (e.g., the whole API crashed)
        if hasattr(result, "error") and result.error:
            error_msg = result.error
        # Otherwise, grab the detailed API params error from the first missing market
        elif result.missing_markets:
            first_missing = list(result.missing_markets.values())[0]
            error_msg = first_missing.error

        # 3. Log it to the database
        self.db.upsert_betfair_meta(
            match_id=match_id,
            betfair_id=None,
            name=f"{request.home} v {request.away}",
            strategy=strategy,
            kickoff_time=request.date,
            status="NOT_FOUND",
            error_log=error_msg,
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
            safe_country = COUNTRY_MAP.get(country, "GB")

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

            if result and result.valid_markets:
                self._succes_process_match(match_id, result, kickoff_dt)
            else:
                self._fail_process_match(match_id, request, result)

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
    producer.run(tournament_filters=[54], limit_filter=20)

    # TODO: 2026-05-11
    # Progress bar for the search overall,  Progress bar for the search overall,
    # More than one file found --- how to deal with that print, / does this cause us an issue
# 5587372
