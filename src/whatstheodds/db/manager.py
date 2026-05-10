# src/whatstheodds/manager.py

from datetime import datetime
from typing import Any, List, Optional, Tuple

from sqlalchemy import Table, create_engine, text
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

    def get_unporcessed_matches(
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
        """

        params: dict[str, str] = {}

        if tournament_ids:
            query_str += " AND e.tournament_id IN :t_ids"
            params["t_ids"] = tuple(tournament_ids)
        if season_ids:
            query_str += " AND e.season_id IN :s_ids"
            params["s_ids"] = tuple(season_ids)
        if limit:
            query_str += " LIMIT :n_limit"
            params["n_limit"]

        with self.SessionLocal() as session:
            return session.execute(text(query_str), params).fetchall()

    def upsert_betfair_meta(
        self, match_id: int, betfair_id: str, name: str, strategy: str
    ):
        """Saves the Betfair match metadata."""

        meta = BetfairMatchMeta(
            match_id=match_id,
            betfair_event_id=betfair_id,
            betfair_event_name=name,
            search_strategy_used=strategy,
            search_date_matched=datetime.utcnow(),
            is_verified=True,
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
            for m_id, m_type in markets_list:
                market = BetfairMarket(
                    market_id=m_id,
                    match_id=match_id,
                    market_type=m_type,
                    status="PENDING",
                )
                session.add(market)
            session.commit()
