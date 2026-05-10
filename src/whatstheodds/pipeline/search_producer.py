# src/whatstheodds/pipeline/search_producer.py

import logging
import os
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from whatstheodds.betfair.dataclasses import BetfairSearchRequest, BetfairSearchResult
from whatstheodds.betfair.search_engine import BetfairSearchEngine
from whatstheodds.configs import load_config
from whatstheodds.db.models import BetfairMarket, BetfairMatchMeta
from whatstheodds.mappers.match_mapper import match_mapper

logger = logging.getLogger(__name__)
