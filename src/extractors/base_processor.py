# extractors/base_processor.py
"""
Base abstract class for all market processors
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple

from omegaconf import DictConfig

from src.utils import load_config

logger = logging.getLogger(__name__)


class BaseMarketProcessor(ABC):
    """Base abstract class for processing different types of market odds"""

    def __init__(self, market_type: str, cfg: Optional[DictConfig] = None):
        self.market_type = market_type

        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]

    @abstractmethod
    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process odds data and return structured data"""
        pass

    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """Return list of column names this processor outputs"""
        pass

    def _build_runner_mapping(self, data: List[Dict]) -> Dict:
        """Map runner IDs to their roles (common functionality)"""
        runner_mapping = {}

        for update in data:
            if "mc" not in update:
                continue

            for market_change in update["mc"]:
                if "marketDefinition" not in market_change:
                    continue

                market_def = market_change["marketDefinition"]
                self._process_market_definition(market_def, runner_mapping)

        return runner_mapping

    @abstractmethod
    def _process_market_definition(
        self, market_def: Dict, runner_mapping: Dict
    ) -> None:
        """Process market definition to identify runners (implemented by subclasses)"""
        pass

    def _extract_ltp_data(
        self, data: List[Dict], runner_mapping: Dict, result: Dict
    ) -> None:
        """Extract LTP (Last Traded Price) data for markets"""
        timestamps_seen: Set = set()

        for update in data:
            timestamp = update.get("pt", None)

            # Skip duplicate timestamps
            if timestamp in timestamps_seen:
                continue

            ltp_values, timestamp_to_add = self._process_update(update, runner_mapping)

            if timestamp_to_add:
                self._add_data_point(timestamp, ltp_values, timestamps_seen, result)

    def _process_update(self, update: Dict, runner_mapping: Dict) -> Tuple[Dict, bool]:
        """Process a single update to extract LTP values"""
        ltp_values = self._create_empty_ltp_values()
        timestamp_to_add = False

        if "mc" not in update:
            return ltp_values, timestamp_to_add

        for market_change in update["mc"]:
            if "rc" not in market_change:
                continue

            timestamp_to_add = (
                self._process_runner_changes(market_change, runner_mapping, ltp_values)
                or timestamp_to_add
            )

        return ltp_values, timestamp_to_add

    def _process_runner_changes(
        self, market_change: Dict, runner_mapping: Dict, ltp_values: Dict
    ) -> bool:
        """Process runner changes and update LTP values"""
        timestamp_to_add = False

        for runner_change in market_change["rc"]:
            runner_id = runner_change.get("id", "")
            if runner_id in runner_mapping and "ltp" in runner_change:
                role = runner_mapping[runner_id]
                ltp_values[role] = runner_change["ltp"]
                timestamp_to_add = True

        return timestamp_to_add

    @abstractmethod
    def _create_empty_ltp_values(self) -> Dict:
        """Create an empty dictionary for LTP values (implemented by subclasses)"""
        pass

    @abstractmethod
    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        """Add a data point to the result (implemented by subclasses)"""
        pass
