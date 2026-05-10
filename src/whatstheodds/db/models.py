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
from sqlalchemy.sql import func

# Create a specific BAse for our betfair models
Base = declarative_base()


class BetfairMatchMeta(Base):
    __tablename__ = "match_meta"
    __table_args__ = {"schema": "betfair"}

    # foregin key directly to the existing sofascrape_db event table
    # The Identifiers
    match_id = Column(Integer, primary_key=True)  # SofaScore Match ID
    betfair_event_id = Column(String, index=True)
    betfair_event_name = Column(String)

    # Discovery Info
    kickoff_time = Column(DateTime(timezone=True))
    search_strategy_used = Column(String)
    search_date_matched = Column(DateTime(timezone=True), server_default=func.now())

    # The "Safety Nets" (The stuff I added that helps with the 400 errors)
    is_verified = Column(Boolean, default=False)
    status = Column(String, default="PENDING")  # PENDING, SUCCESS, RETRY, FAILED
    retry_count = Column(Integer, default=0)
    error_log = Column(Text)  # This is where you save that "Status code 400" message

    last_updated = Column(DateTime, onupdate=func.now())

    def __repr__(self):
        return f"<BetfairMatchMeta(match_id={self.match_id}, status={self.status})>"


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
