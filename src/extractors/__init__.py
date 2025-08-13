# extractors/__init__.py
"""
Extractor module for processing Betfair odds data from compressed files
"""

from .match_extractor import MatchExtractor
from .odds_processor import OddsDataProcessor

__all__ = ["MatchExtractor", "OddsDataProcessor"]
