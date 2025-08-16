# extractors/market_processors/match_odds.py
"""
Processor for Match Odds markets (1X2)
"""

from typing import Dict, List, Optional, Set

from omegaconf import DictConfig

from src.utils import load_config

from ..base_processor import BaseMarketProcessor


class MatchOddsProcessor(BaseMarketProcessor):
    """Processor for Match Odds markets (Home/Away/Draw)"""

    def __init__(self, config: Optional[DictConfig] = None):
        super().__init__("MATCH_ODDS", config)

    def get_output_columns(self) -> List[str]:
        return ["home", "away", "draw"]

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process match odds data"""
        result: Dict = {"timestamps": [], "home": [], "away": [], "draw": []}

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

            self._assign_runner_role(runner_id, runner_name, runner_mapping)

    def _assign_runner_role(
        self, runner_id: int, runner_name: str, runner_mapping: Dict
    ) -> None:
        """Assign a role (home/away/draw) to a runner"""
        if runner_name.lower() == "the draw":
            runner_mapping[runner_id] = "draw"
        elif (
            "home" not in runner_mapping.values() and runner_name.lower() != "the draw"
        ):
            runner_mapping[runner_id] = "home"
        elif (
            "away" not in runner_mapping.values() and runner_name.lower() != "the draw"
        ):
            runner_mapping[runner_id] = "away"

    def _create_empty_ltp_values(self) -> Dict:
        return {"home": None, "away": None, "draw": None}

    def _add_data_point(
        self, timestamp: str, ltp_values: Dict, timestamps_seen: Set, result: Dict
    ) -> None:
        timestamps_seen.add(timestamp)
        result["timestamps"].append(timestamp)
        result["home"].append(ltp_values["home"])
        result["away"].append(ltp_values["away"])
        result["draw"].append(ltp_values["draw"])
