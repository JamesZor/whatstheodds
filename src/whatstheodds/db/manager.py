# src/whatstheodds/manager.py

from datetime import datetime
from typing import Any, List, Optional, Tuple

from sqlalchemy import Table, create_engine, insert, text
from sqlalchemy.orm import sessionmaker

from whatstheodds.configs.config import AppConfig
from whatstheodds.db.models import (
    Base,
    BetfairMarket,
    BetfairMatchMeta,
    MarketOddsHistory,
)


class DatabaseManager:
    def __init__(self, config: AppConfig):
        self.engine = create_engine(config.database.url)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # Reflect the existing public tbales immediately
        # to ensure Foreign Keys work across schemas
        self._reflect_public_tables()

    def _reflect_public_tables(self):
        """Map existing tbales in the public name space schema"""
        Table("events", Base.metadata, autoload_with=self.engine, schema="public")
        Table("tournaments", Base.metadata, autoload_with=self.engine, schema="public")

    def get_unprocessed_matches(
        self,
        tournament_ids: Optional[List[int]] = None,
        season_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Fetches matches from public.events not yet in betfair.match_meta.
        Allows for filtering by torunament or seasons.
        """
        query_str = """
            SELECT e.match_id, e.home_team, e.away_team, e.start_timestamp, t.country 
            FROM public.events e
            LEFT JOIN public.tournaments t ON e.tournament_id = t.tournament_id
            LEFT JOIN betfair.match_meta m ON e.match_id = m.match_id
            WHERE m.match_id IS NULL
            and e.status_type = 'finished'
        """

        params: dict[str, Any] = {}

        if tournament_ids:
            query_str += " AND e.tournament_id IN :t_ids"
            params["t_ids"] = tuple(tournament_ids)
        if season_ids:
            query_str += " AND e.season_id IN :s_ids"
            params["s_ids"] = tuple(season_ids)
        if limit:
            query_str += " LIMIT :n_limit"
            params["n_limit"] = limit

        with self.SessionLocal() as session:
            return session.execute(text(query_str), params).fetchall()

    def upsert_betfair_meta(
        self,
        match_id: int,
        betfair_id: Optional[str],
        name: str,
        strategy: str,
        kickoff_time: Optional[datetime] = None,
        status: str = "SUCCESS",  # NEW
        error_log: Optional[str] = None,  # NEW
    ):
        """Saves the Betfair match metadata (handles successes, failures, and crashes)."""

        meta = BetfairMatchMeta(
            match_id=match_id,
            betfair_event_id=betfair_id,
            betfair_event_name=name,
            kickoff_time=kickoff_time,
            search_strategy_used=strategy,
            search_date_matched=datetime.utcnow() if betfair_id else None,
            is_verified=True if betfair_id else False,
            status=status,  # NEW
            error_log=error_log,  # NEW
        )

        with self.SessionLocal() as session:
            session.merge(meta)
            session.commit()

    def queue_markets(self, match_id: int, markets_list: List[Tuple[str, str]]):
        """
        Takes a list of (market_id, market_type) and adds them to the queue.
        markets_list example: [('1.123', 'MATCH_ODDS'), ('1.456', 'OVER_UNDER_25')]
        """
        with self.SessionLocal() as session:
            for m_id, m_type, f_path in markets_list:
                market = BetfairMarket(
                    market_id=m_id,
                    match_id=match_id,
                    market_type=m_type,
                    file_path=f_path,
                    status="PENDING",
                )
                session.add(market)
            session.commit()

    # --- downloader related methods calls
    def claim_pending_markets(self, limit: int = 10) -> List[Tuple[str, int, str]]:
        """
        Atomically claims a batch of PENDING markets and marks them DOWNLOADING.
        Uses FOR UPDATE SKIP LOCKED to ensure multiple workers don't grab the same jobs.
        Returns: [(market_id, match_id, market_type), ...]
        """

        query = text("""
            UPDATE betfair.markets
            SET status = 'DOWNLOADING', last_updated = NOW()
            WHERE market_id IN (
                SELECT market_id FROM betfair.markets 
                WHERE status = 'PENDING' ORDER BY last_updated ASC
                LIMIT :limit FOR UPDATE SKIP LOCKED
            )
            RETURNING market_id, match_id, market_type, file_path;  -- <-- Added file_path
        """)

        with self.SessionLocal() as session:
            # Execute the atomic claim and fetch the rows we successfully grabbed
            result = session.execute(query, {"limit": limit}).fetchall()
            session.commit()
            return result

    def update_market_status(
        self, market_id: str, status: str, error_log: Optional[str] = None
    ):
        """Updates the queue status (SUCCESS or FAILED) after a download attempt."""
        with self.SessionLocal() as session:
            market = (
                session.query(BetfairMarket)
                .filter(BetfairMarket.market_id == market_id)
                .first()
            )
            if market:
                market.status = status
                if error_log:
                    market.error_log = error_log
                if status == "FAILED":
                    market.retry_count += 1
            session.commit()

    def bulk_insert_odds(self, odds_records: List[dict]):
        """
        Takes a list of dictionaries and inserts them all at once into odds_history.
        Expected format: [{"match_id": 1, "market_id": "1.23", "odds_data": {...}}, ...]
        """
        if not odds_records:
            return

        with self.SessionLocal() as session:
            # bulk_insert_mappings is incredibly fast for thousands of rows
            session.execute(insert(MarketOddsHistory), odds_records)
            session.commit()

    def requeue_failed_markets(self, max_retries: int = 3) -> int:
        """
        Resets FAILED markets to PENDING if they are under the retry limit.
        Returns the number of rows updated.
        """
        query = text("""
            UPDATE betfair.markets
            SET status = 'PENDING'
            WHERE status = 'FAILED' AND retry_count < :max_retries
        """)
        with self.SessionLocal() as session:
            result = session.execute(query, {"max_retries": max_retries})
            session.commit()
            return result.rowcount
