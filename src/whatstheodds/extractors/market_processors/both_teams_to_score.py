# extractors/market_processors/both_teams_to_score.py
"""
Processor for Both Teams to Score markets
"""

from typing import Dict, List, Optional, Set

from omegaconf import DictConfig

from ..base_processor import BaseMarketProcessor


class BothTeamsToScoreProcessor(BaseMarketProcessor):
    """Processor for Both Teams to Score markets"""

    def __init__(self, config: Optional[DictConfig] = None):
        super().__init__("BOTH_TEAMS_TO_SCORE", config)

    def get_output_columns(self) -> List[str]:
        """Return list of column names this processor outputs"""
        return ["btts_yes", "btts_no"]

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process both teams to score odds data"""
        result: Dict = {"timestamps": [], "btts_yes": [], "btts_no": []}

        runner_mapping = self._build_runner_mapping(data)
        self._extract_ltp_data(data, runner_mapping, result)

        return result

    def _process_market_definition(
        self, market_def: Dict, runner_mapping: Dict
    ) -> None:
        """Process market definition to identify runners"""
        if market_def.get("marketType") != "BOTH_TEAMS_TO_SCORE":
            return

        for runner in market_def.get("runners", []):
            runner_id = runner.get("id")
            runner_name = runner.get("name", "").lower()

            if runner_name == "yes":
                runner_mapping[runner_id] = "btts_yes"
            elif runner_name == "no":
                runner_mapping[runner_id] = "btts_no"

    def _create_empty_ltp_values(self) -> Dict:
        """Create an empty dictionary for LTP values"""
        return {"btts_yes": None, "btts_no": None}

    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        """Add a data point to the result"""
        timestamps_seen.add(timestamp)
        result["timestamps"].append(timestamp)
        result["btts_yes"].append(ltp_values.get("btts_yes"))
        result["btts_no"].append(ltp_values.get("btts_no"))
