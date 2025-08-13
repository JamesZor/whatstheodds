import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)


########################################
# HACK: dataclass
########################################
@dataclass
class BetfairSearchRequest:
    sofa_match_id: int
    home: str
    away: str
    date: datetime
    country: str


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


# TODO: - class needs the file path ( which is used in the downloader )
@dataclass
class BetfairSearchResult:
    match_id: Optional[str] = None
    markets: Dict[str, BetfairSearchSingleMarketResult] = {}
