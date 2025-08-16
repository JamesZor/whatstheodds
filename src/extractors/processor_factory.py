# extractors/processor_factory.py
"""
Factory for creating appropriate market processors
"""

import logging
from typing import Dict, List, Optional

from omegaconf import DictConfig

from src.utils import load_config

from .market_processors import (
    CorrectScoreProcessor,
    HalfTimeMatchOddsProcessor,
    HalfTimeOverUnderProcessor,
    MatchOddsProcessor,
    OverUnderProcessor,
)

logger = logging.getLogger(__name__)


class ProcessorFactory:
    """Factory class to create appropriate processor for each market type"""

    def __init__(self, cfg: Optional[DictConfig] = None):
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]
        self._processors: Dict = {}
        self._initialize_processors()

    def _initialize_processors(self):
        """Initialize all available processors based on config"""
        # Match odds processors
        self._processors["MATCH_ODDS"] = MatchOddsProcessor(self.cfg)
        self._processors["HALF_TIME"] = HalfTimeMatchOddsProcessor(self.cfg)

        self._processors["CORRECT_SCORE"] = CorrectScoreProcessor(self.cfg)

        # Over/Under processors for different goal lines
        goal_lines = ["0.5", "1.5", "2.5", "3.5", "4.5"]
        for goal_line in goal_lines:
            # Full time
            market_key = f"OVER_UNDER_{goal_line.replace('.', '')}"
            self._processors[market_key] = OverUnderProcessor(goal_line, self.cfg)

            # Alternative naming
            market_key_alt = f"OVER_UNDER_{goal_line.replace('.', '_')}"
            self._processors[market_key_alt] = OverUnderProcessor(goal_line, self.cfg)

            # Half time
            ht_market_key = f"FIRST_HALF_GOALS_{goal_line.replace('.', '')}"
            self._processors[ht_market_key] = HalfTimeOverUnderProcessor(
                goal_line, self.cfg
            )

    def get_processor(self, market_type: str):
        """Get processor for specific market type"""
        processor = self._processors.get(market_type)
        if not processor:
            logger.warning(f"No processor found for market type: {market_type}")
        return processor

    def get_supported_markets(self) -> List[str]:
        """Get list of all supported market types"""
        return list(self._processors.keys())
