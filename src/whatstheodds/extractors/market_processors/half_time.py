# extractors/market_processors/half_time.py
"""
Processors for Half-Time markets
"""
import logging
from typing import Dict, List, Optional, Set

from omegaconf import DictConfig

from ..base_processor import BaseMarketProcessor

logger = logging.getLogger(__name__)


class HalfTimeMatchOddsProcessor(BaseMarketProcessor):
    """Processor for Half Time Match Odds markets"""

    def __init__(self, config: Optional[Dict] = None):
        super().__init__("HALF_TIME", config)

    def get_output_columns(self) -> List[str]:
        return ["ht_home", "ht_away", "ht_draw"]

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process half time match odds data"""
        result: Dict = {"timestamps": [], "ht_home": [], "ht_away": [], "ht_draw": []}

        runner_mapping = self._build_runner_mapping(data)
        self._extract_ltp_data(data, runner_mapping, result)

        return result

    def _process_market_definition(
        self, market_def: Dict, runner_mapping: Dict
    ) -> None:
        """Process market definition to identify runners"""
        existing_roles = set(runner_mapping.values())

        for runner in market_def.get("runners", []):
            runner_id = runner.get("id")
            runner_name = runner.get("name", "")

            if runner_name.lower() == "the draw":
                runner_mapping[runner_id] = "ht_draw"
            elif "ht_home" not in existing_roles and runner_name.lower() != "the draw":
                runner_mapping[runner_id] = "ht_home"
                existing_roles.add("ht_home")
            elif "ht_away" not in existing_roles and runner_name.lower() != "the draw":
                runner_mapping[runner_id] = "ht_away"
                existing_roles.add("ht_away")

    def _create_empty_ltp_values(self) -> Dict:
        return {"ht_home": None, "ht_away": None, "ht_draw": None}

    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        timestamps_seen.add(timestamp)
        result["timestamps"].append(timestamp)
        result["ht_home"].append(ltp_values.get("ht_home"))
        result["ht_away"].append(ltp_values.get("ht_away"))
        result["ht_draw"].append(ltp_values.get("ht_draw"))


class HalfTimeOverUnderProcessor(BaseMarketProcessor):
    """Processor for Half Time Over/Under markets"""

    def __init__(self, goal_line: str, config: Optional[Dict] = None):
        super().__init__(f"FIRST_HALF_GOALS_{goal_line.replace('.', '')}", config)
        self.goal_line = goal_line
        self.goal_line_str = goal_line.replace(".", "_")
        self.over_key = f"ht_over_{self.goal_line_str}"
        self.under_key = f"ht_under_{self.goal_line_str}"

    def get_output_columns(self) -> List[str]:
        return [self.over_key, self.under_key]

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process half time over/under odds data"""
        result: Dict = {"timestamps": [], self.over_key: [], self.under_key: []}

        runner_mapping = self._build_runner_mapping(data)
        self._extract_ltp_data(data, runner_mapping, result)

        return result

    def _process_market_definition(
        self, market_def: Dict, runner_mapping: Dict
    ) -> None:
        """Process market definition to identify runners"""
        for runner in market_def.get("runners", []):
            runner_id = runner.get("id")
            runner_name = runner.get("name", "")

            if "over" in runner_name.lower():
                runner_mapping[runner_id] = "over"
            elif "under" in runner_name.lower():
                runner_mapping[runner_id] = "under"

    def _create_empty_ltp_values(self) -> Dict:
        return {"over": None, "under": None}

    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        timestamps_seen.add(timestamp)
        result["timestamps"].append(timestamp)
        result[self.over_key].append(ltp_values.get("over"))
        result[self.under_key].append(ltp_values.get("under"))


class HalfTimeScoreProcessor(BaseMarketProcessor):
    """Processor for Half Time Score markets"""

    def __init__(self, config: Optional[DictConfig] = None):
        super().__init__("HALF_TIME_SCORE", config)
        self.score_columns: List[str] = []  # Will be populated dynamically
        self.special_columns = {
            "ht_any_unquoted": "Any Unquoted",
        }

    def get_output_columns(self) -> List[str]:
        """Return list of column names this processor outputs"""
        # Return cached columns if available
        if self.score_columns:
            return self.score_columns

        # Default common half-time score lines if not yet processed
        default_scores = [
            "ht_0_0",
            "ht_0_1",
            "ht_0_2",
            "ht_1_0",
            "ht_1_1",
            "ht_1_2",
            "ht_2_0",
            "ht_2_1",
            "ht_2_2",
            "ht_any_unquoted",
        ]
        return default_scores

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process half time score odds data"""
        # Build runner mapping first to identify all score lines
        runner_mapping = self._build_runner_mapping(data)

        # Get all unique score columns from the mapping
        self.score_columns = sorted(list(set(runner_mapping.values())))

        # Initialize result dictionary with timestamps and all score columns
        result: Dict = {"timestamps": []}
        for col in self.score_columns:
            result[col] = []

        # Extract LTP data
        self._extract_ltp_data(data, runner_mapping, result)

        return result

    def _process_market_definition(
        self, market_def: Dict, runner_mapping: Dict
    ) -> None:
        """Process market definition to identify runners"""
        if market_def.get("marketType") != "HALF_TIME_SCORE":
            return

        for runner in market_def.get("runners", []):
            runner_id = runner.get("id")
            runner_name = runner.get("name", "")

            # Map special outcomes
            if "any unquoted" in runner_name.lower():
                runner_mapping[runner_id] = "ht_any_unquoted"
            else:
                # Process regular score lines (e.g., "0 - 0" -> "ht_0_0")
                score_formatted = self._format_score_name(runner_name)
                if score_formatted:
                    runner_mapping[runner_id] = score_formatted

    def _format_score_name(self, score_name: str) -> Optional[str]:
        """
        Format score name from "0 - 0" to "ht_0_0"

        Args:
            score_name: Raw score name from Betfair

        Returns:
            Formatted score name with ht_ prefix or None if invalid
        """
        try:
            # Handle format like "0 - 0", "1 - 2", etc.
            if " - " in score_name:
                parts = score_name.split(" - ")
                if len(parts) == 2:
                    home_score = parts[0].strip()
                    away_score = parts[1].strip()
                    # Validate that both parts are numbers
                    if home_score.isdigit() and away_score.isdigit():
                        return f"ht_{home_score}_{away_score}"
        except Exception as e:
            logger.warning(f"Error formatting score name '{score_name}': {str(e)}")

        return None

    def _create_empty_ltp_values(self) -> Dict:
        """Create an empty dictionary for LTP values"""
        # Create dict with None for all known score columns
        ltp_values: Dict = {}
        for col in self.score_columns:
            ltp_values[col] = None
        return ltp_values

    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        """Add a data point to the result"""
        timestamps_seen.add(timestamp)
        result["timestamps"].append(timestamp)

        # Add values for each score column
        for col in self.score_columns:
            if col in result:  # Only add if column exists in result
                result[col].append(ltp_values.get(col))

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

            ltp_values = self._create_empty_ltp_values()
            timestamp_to_add = False

            if "mc" in update:
                for market_change in update["mc"]:
                    # Check if market is settled (half time reached)
                    if "marketDefinition" in market_change:
                        market_def = market_change["marketDefinition"]
                        if market_def.get("status") == "CLOSED":
                            # Extract the winning score at half time
                            for runner in market_def.get("runners", []):
                                if runner.get("status") == "WINNER":
                                    winner_name = runner.get("name", "")
                                    if "any unquoted" not in winner_name.lower():
                                        winner_score = self._format_score_name(
                                            winner_name
                                        )
                                        if winner_score:
                                            logger.info(
                                                f"Half time score: {winner_name}"
                                            )

                    # Process runner changes for odds updates
                    if "rc" in market_change:
                        for runner_change in market_change["rc"]:
                            runner_id = runner_change.get("id", "")
                            if runner_id in runner_mapping and "ltp" in runner_change:
                                score_col = runner_mapping[runner_id]
                                ltp_values[score_col] = runner_change["ltp"]
                                timestamp_to_add = True

            if timestamp_to_add:
                self._add_data_point(timestamp, ltp_values, timestamps_seen, result)
