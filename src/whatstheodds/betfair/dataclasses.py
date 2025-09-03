import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)


########################################
#  dataclass
########################################
@dataclass
class BetfairSearchRequest:
    sofa_match_id: int
    home: str
    away: str
    date: datetime
    country: str

    def to_dict(self) -> Dict:
        return {
            "sofa_match_id": int(self.sofa_match_id),
            "home": self.home,
            "away": self.away,
            "data": str(self.date),
            "country": self.country,
        }


@dataclass
class BetfairSearchSingleMarketResult:
    """Dataclass for a search with single market
    /xds_nfs/edp_processed/BASIC/2022/Aug/31/31679991/1.202358758.bz2'
    """

    strategy_used: str
    market_type: str
    file: Optional[str] = None
    match_id: Optional[str] = None
    market_id: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "strategy_used": self.strategy_used,
            "market_type": self.market_type,
            "file": self.file,
            "match_id": self.match_id,
            "market_id": self.market_id,
            "error": self.error,
        }

    @classmethod
    def from_path_string(cls, file: str, strategy_used_name: str, market_type: str):
        file_name_split: List[str] = file.split("/")
        match_id = file_name_split[-2]
        market_id = file_name_split[-1].split(".")[1]
        return cls(
            strategy_used=strategy_used_name,
            market_type=market_type,
            file=file,
            market_id=market_id,
            match_id=match_id,
        )

    def toJSON(self):
        """Legacy method - use to_dict() instead"""
        return json.dumps(self.to_dict(), sort_keys=True, indent=4)


@dataclass
class BetfairSearchResult:
    match_id: Optional[str] = None
    valid_markets: Dict[str, BetfairSearchSingleMarketResult] = field(
        default_factory=dict
    )
    missing_markets: Dict[str, BetfairSearchSingleMarketResult] = field(
        default_factory=dict
    )

    @classmethod
    def from_results_list(cls, results: List[BetfairSearchSingleMarketResult]):
        # Create markets dict mapping market_type to result
        valid_markets = {
            result.market_type: result
            for result in results
            if result.match_id is not None
        }

        missing_markets = {
            result.market_type: result for result in results if result.match_id is None
        }

        # Get match_id from first successful result, or None if all failed
        match_id = None
        for result in results:
            if result.match_id is not None:
                match_id = result.match_id
                break

        return cls(
            match_id=match_id,
            valid_markets=valid_markets,
            missing_markets=missing_markets,
        )

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


@dataclass
class BetfairSearchResultMultiDate:
    to_date: Optional[datetime] = None
    from_date: Optional[datetime] = None
    events: Dict[str, BetfairSearchSingleMarketResult] = field(default_factory=dict)

    @classmethod
    def from_results_list(
        cls,
        results: List[BetfairSearchSingleMarketResult],
        to_date: datetime,
        from_date: datetime,
    ):
        # Create markets dict mapping market_type to result
        events = {
            result.match_id: result for result in results if result.match_id is not None
        }

        return cls(to_date=to_date, from_date=from_date, events=events)
