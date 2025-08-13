# extractors/market_processors/half_time.py
"""
Processors for Half-Time markets
"""

from typing import Dict, List, Optional, Set

from ..base_processor import BaseMarketProcessor


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
