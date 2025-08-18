# extractors/market_processors/over_under.py
"""
Processor for Over/Under markets
"""

from typing import Dict, List, Optional, Set

from ..base_processor import BaseMarketProcessor


class OverUnderProcessor(BaseMarketProcessor):
    """Processor for Over/Under goals markets"""

    def __init__(self, goal_line: str, config: Optional[Dict] = None):
        super().__init__(f"OVER_UNDER_{goal_line.replace('.', '')}", config)
        self.goal_line = goal_line
        self.goal_line_str = goal_line.replace(".", "_")
        self.over_key = f"over_{self.goal_line_str}"
        self.under_key = f"under_{self.goal_line_str}"

    def get_output_columns(self) -> List[str]:
        return [self.over_key, self.under_key]

    def process(self, data: List[Dict]) -> Dict[str, List]:
        """Process over/under odds data"""
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
