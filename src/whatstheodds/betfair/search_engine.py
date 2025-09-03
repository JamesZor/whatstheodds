import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import betfairlightweight
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.utils import load_config

from .api_client import setup_betfair_api_client
from .dataclasses import (
    BetfairSearchRequest,
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)
from .search_strategies import (
    BaseSearchStrategy,
    ExactDateTeamSearch,
    ExtendDateTeamSearch,
)

logger = logging.getLogger(__name__)


########################################
# Main class
########################################
class BetfairSearchEngine:
    """
    Searches for matches on Betfair with multiple strategies
    """

    def __init__(
        self,
        api_client: Optional[betfairlightweight.APIClient] = None,
        cfg: Optional[DictConfig] = None,
    ):

        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]

        self.client: betfairlightweight.APIClient = (
            setup_betfair_api_client() if api_client is None else api_client
        )

        # Init strategy attributes
        self.search_strategy: BaseSearchStrategy
        self.extended_search_strategy: Optional[BaseSearchStrategy] = None

        # Set up strategies from config
        self.set_search_strategies()

    def set_search_strategies(self) -> None:
        """
        Set up search strategies based on configuration
        Sets self.search_strategy and self.extended_search_strategy
        """
        search_strategies_mapping: Dict[str, Type[BaseSearchStrategy]] = {
            "ExactDateTeamSearch": ExactDateTeamSearch,
            "ExtendDateTeamSearch": ExtendDateTeamSearch,
            # TODO: Add more strategies
        }

        # Set primary search strategy
        primary_strategy_name = self.cfg.search.primary_strategy
        if primary_strategy_name not in search_strategies_mapping:
            raise ValueError(f"Unknown primary strategy: {primary_strategy_name}")

        self.search_strategy = search_strategies_mapping[primary_strategy_name](
            cfg=self.cfg, client=self.client
        )
        logger.info(f"Primary search strategy set to: {primary_strategy_name}")

        # Set extended search strategy (optional)
        extended_strategy_name = getattr(self.cfg.search, "extended_strategy", None)
        if (
            extended_strategy_name
            and extended_strategy_name in search_strategies_mapping
        ):
            self.extended_search_strategy = search_strategies_mapping[
                extended_strategy_name
            ](cfg=self.cfg, client=self.client)
            logger.info(f"Extended search strategy set to: {extended_strategy_name}")
        else:
            self.extended_search_strategy = None
            if extended_strategy_name:
                logger.warning(
                    f"Extended strategy '{extended_strategy_name}' not found in mapping"
                )

    def search_match(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        """
        Search for a match using multiple strategies

        Returns:
            SearchResult with:
                - match_id: Optional[str]
                - confidence: float (0-1)
                - strategy_used: str
                - search_attempts: List[SearchAttempt]
        """
        return self.search_strategy.search(search_request)

    def search_main(self, search_request: BetfairSearchRequest) -> BetfairSearchResult:
        """
        Main search method that tries initial search and retries failed ones with extended strategy
        """
        # First search using primary strategy
        logger.info(f"searching markets: {self.cfg.betfair_football.markets}.")
        search_results = self.search_strategy.search_over_config_markets(search_request)

        # If we have an extended search strategy, retry failed searches
        if self.extended_search_strategy is not None:
            for i, result in enumerate(search_results):
                if result.match_id is None:  # This is a failed result
                    try:
                        new_result = (
                            self.extended_search_strategy._search_per_market_type(
                                search_request=search_request,
                                market_type=result.market_type,
                            )
                        )

                        # Replace with new result if it succeeded
                        if new_result.match_id is not None:
                            search_results[i] = new_result
                            logger.info(
                                f"Successfully retried {result.market_type} with extended strategy"
                            )

                    except Exception as e:
                        logger.warning(
                            f"Extended search failed for {result.market_type}: {str(e)}"
                        )

                # Keep the original failed result
        return BetfairSearchResult.from_results_list(search_results, search_request)  # type: ignore[no-any-return]
