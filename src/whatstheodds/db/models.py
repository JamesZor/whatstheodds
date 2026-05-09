# src/whatstheodds/db.models.py


from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

# Create a specific BAse for our betfair models
Base = declarative_base()


class BetfairMatchMeta(Base):
    __tablename__ = "match_meta"
    __table_args__ = {"schema": "betfair"}

    # foregin key directly to the existing sofascrape_db event table
    match_id = Column(Integer, ForeignKey("public.events.match_id"), primary_key=True)

    # Betfair specific data
    betfair_event_id = Column(String, unique=True, nullable=False)
    betfair_event_name = Column(String, nullable=True)
    kickoff_time = Column(DateTime(timezone=True), nullable=True)

    # Search Audit Data ( To see how/if we found it)
    search_strategy_used = Column(String)
    search_date_matched = Column(DateTime(timezone=True))
    is_verified = Column(Boolean, default=False)

    # relationships
    markets = relationship("BetfairMarket", back_populates="match")


class BetfairMarket(Base):
    __tablename__ = "markets"
    __table_args__ = {"schema": "betfair"}

    market_id = Column(String, primary_key=True)
    match_id = Column(
        Integer, ForeignKey("betfair.match_meta.match_id"), nullable=False
    )

    market_type = Column(String, nullable=False)  # e.g 'MATCH_ODDS', 'OVER_UNDER'

    # state Machine ( queue ) Columns
    status = Column(
        String, nullable=False, default="PENDING"
    )  # PENDING, DOWNLOADING, EXTRACTED, FAILED
    retry_count = Column(Integer, default=0)
    error_log = Column(String, nullable=True)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    match = relationship("BetfairMatchMeta", back_populates="markets")
    odds_history = relationship("MarketOddsHistory", back_populates="market")


class MarketOddsHistory(Base):
    __tablename__ = "odds_history"
    __table_args__ = {"schema": "betfair"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("public.events.match_id"), nullable=False)
    market_id = Column(String, ForeignKey("betfair.markets.market_id"), nullable=False)

    odds_data = Column(JSONB, nullable=False)

    # Relationships
    market = relationship("BetfairMarket", back_populates="odds_history")
