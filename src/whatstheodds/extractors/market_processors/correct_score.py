# extractors/market_processors/correct_score.py
"""
Processor for Correct Score markets
"""
import logging
from typing import Dict, List, Optional, Set

from omegaconf import DictConfig

from whatstheodds.utils import load_config

from ..base_processor import BaseMarketProcessor

logger = logging.getLogger(__name__)


class CorrectScoreProcessor(BaseMarketProcessor):
    """Processor for Correct Score markets"""

    def __init__(self, config: Optional[DictConfig] = None):
        super().__init__("CORRECT_SCORE", config)
        self.score_columns: List[str] = []  # Will be populated dynamically
        self.special_columns = {
            "any_other_home": "Any Other Home Win",
            "any_other_away": "Any Other Away Win",
            "any_other_draw": "Any Other Draw",
        }

    def get_output_columns(self) -> List[str]:
        """Return list of column names this processor outputs"""
        # Return cached columns if available
        if self.score_columns:
            return self.score_columns

        # Default common score lines if not yet processed
        default_scores = [
            "0_0",
            "0_1",
            "0_2",
            "0_3",
            "1_0",
            "1_1",
            "1_2",
            "1_3",
            "2_0",
            "2_1",
            "2_2",
            "2_3",
            "3_0",
            "3_1",
            "3_2",
            "3_3",
        ]
        # Add special outcome columns
        default_scores.extend(["any_other_home", "any_other_away", "any_other_draw"])
        return default_scores

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process correct score odds data"""
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
        if market_def.get("marketType") != "CORRECT_SCORE":
            return

        for runner in market_def.get("runners", []):
            runner_id = runner.get("id")
            runner_name = runner.get("name", "")

            # Map special outcomes
            if runner_name == "Any Other Home Win":
                runner_mapping[runner_id] = "any_other_home"
            elif runner_name == "Any Other Away Win":
                runner_mapping[runner_id] = "any_other_away"
            elif runner_name == "Any Other Draw":
                runner_mapping[runner_id] = "any_other_draw"
            else:
                # Process regular score lines (e.g., "0 - 0" -> "0_0")
                score_formatted = self._format_score_name(runner_name)
                if score_formatted:
                    runner_mapping[runner_id] = score_formatted

    def _format_score_name(self, score_name: str) -> Optional[str]:
        """
        Format score name from "0 - 0" to "0_0"

        Args:
            score_name: Raw score name from Betfair

        Returns:
            Formatted score name or None if invalid
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
                        return f"{home_score}_{away_score}"
        except Exception as e:
            logger.warning(f"Error formatting score name '{score_name}': {str(e)}")

        return None

    def _create_empty_ltp_values(self) -> Dict:
        """Create an empty dictionary for LTP values"""
        # Create dict with None for all known score columns
        ltp_values = {}
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
                    # Check if market is settled (game ended)
                    if "marketDefinition" in market_change:
                        market_def = market_change["marketDefinition"]
                        if market_def.get("status") == "CLOSED":
                            # Extract the winning score
                            for runner in market_def.get("runners", []):
                                if runner.get("status") == "WINNER":
                                    winner_name = runner.get("name", "")
                                    winner_score = self._format_score_name(winner_name)
                                    if winner_score:
                                        logger.info(
                                            f"Match ended with score: {winner_name}"
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
