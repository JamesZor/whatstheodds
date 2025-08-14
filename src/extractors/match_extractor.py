# extractors/match_extractor.py
"""
Main extractor for processing match data from compressed Betfair files
"""

import bz2
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from omegaconf import DictConfig

from src.utils import load_config

from .odds_processor import OddsDataProcessor
from .processor_factory import ProcessorFactory

logger = logging.getLogger(__name__)


class MatchExtractor:
    """Extract and process odds data from Betfair match files"""

    def __init__(self, config: Optional[DictConfig] = None):
        """
        Initialize with configuration

        Args:
            config: OmegaConf configuration object
        """
        self.config: DictConfig = load_config() if config is None else config  # type: ignore[assignment]
        self.temp_dir = Path(config.storage.temp)
        self.processor_factory = ProcessorFactory(config)
        self.odds_processor = OddsDataProcessor(config)
        self.file_cache: Dict = {}

        # Get market types from config
        self.target_markets = config.betfair_football.markets

    def extract_match(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Extract all odds data for a match from temp directory

        Args:
            match_id: Betfair match ID

        Returns:
            Dictionary containing match info and processed odds data
        """
        match_path = self.temp_dir / str(match_id)

        if not match_path.exists():
            logger.error(f"Match directory not found: {match_path}")
            return None

        logger.info(f"Extracting match {match_id} from {match_path}")

        try:
            # Clear cache for new match
            self.file_cache = {}

            # Load all .bz2 files into cache
            market_files = self._cache_market_files(match_path)

            if not market_files:
                logger.warning(f"No market files found for match {match_id}")
                return None

            # Extract match information
            match_info = self._extract_match_info(market_files)

            # Identify available market types
            market_types = self._identify_market_types(market_files)

            # Process each market
            processed_markets = self._process_markets(market_types, market_files)

            # Merge all market data into single DataFrame
            odds_data = self._merge_market_data(processed_markets)

            # Clean and prepare data
            if self.config.get("processing", {}).get("clean_data", True):
                odds_data = self.odds_processor.clean_odds_data(
                    odds_data, match_info.get("kick_off_time")
                )

            # Clear cache to free memory
            self.file_cache = {}

            return {
                "match_id": match_id,
                "match_info": match_info,
                "odds_data": odds_data,
                "markets_processed": list(processed_markets.keys()),
                "extraction_time": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error extracting match {match_id}: {str(e)}")
            return None

    def _cache_market_files(self, match_path: Path) -> Dict[str, str]:
        """Load all .bz2 files into cache"""
        market_files = {}

        for file_path in match_path.glob("*.bz2"):
            try:
                # Extract market ID from filename (e.g., "1.202358758.bz2" -> "202358758")
                parts = file_path.name.split(".")
                if len(parts) >= 3:
                    market_id = parts[1]
                    str_path = str(file_path)

                    # Load file into cache
                    self._load_file_to_cache(str_path)
                    market_files[market_id] = str_path

            except Exception as e:
                logger.warning(f"Error processing file {file_path}: {str(e)}")
                continue

        logger.info(f"Cached {len(market_files)} market files")
        return market_files

    def _load_file_to_cache(self, file_path: str) -> List[Dict]:
        """Load and decompress a .bz2 file into cache"""
        if file_path in self.file_cache:
            return self.file_cache[file_path]

        try:
            with bz2.open(file_path, "rt", encoding="utf-8") as f:
                data = [json.loads(line) for line in f if line.strip()]
                self.file_cache[file_path] = data
                return data
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {str(e)}")
            self.file_cache[file_path] = []
            return []

    def _extract_match_info(self, market_files: Dict[str, str]) -> Dict[str, Any]:
        """Extract match information from market files"""
        match_info = {
            "home_team": None,
            "away_team": None,
            "kick_off_time": None,
            "event_name": None,
            "competition": None,
        }

        # Try each file until we find match info
        for market_id, file_path in market_files.items():
            data = self.file_cache.get(file_path, [])

            for update in data:
                if "mc" not in update:
                    continue

                for market_change in update["mc"]:
                    if "marketDefinition" not in market_change:
                        continue

                    market_def = market_change["marketDefinition"]

                    # Extract basic info
                    if "eventName" in market_def:
                        match_info["event_name"] = market_def["eventName"]

                        # Try to parse team names
                        parts = market_def["eventName"].split(" v ")
                        if len(parts) == 2:
                            match_info["home_team"] = parts[0].strip()
                            match_info["away_team"] = parts[1].strip()

                    if "marketTime" in market_def:
                        match_info["kick_off_time"] = market_def["marketTime"]

                    if "competition" in market_def:
                        match_info["competition"] = market_def.get(
                            "competition", {}
                        ).get("name")

                    # If this is MATCH_ODDS, try to get better team names
                    if (
                        market_def.get("marketType") == "MATCH_ODDS"
                        and "runners" in market_def
                    ):
                        runners = market_def["runners"]
                        teams = [
                            r["name"]
                            for r in runners
                            if r["name"].lower() != "the draw"
                        ]
                        if len(teams) >= 2:
                            match_info["home_team"] = teams[0]
                            match_info["away_team"] = teams[1]
                            return match_info

            # If we found enough info, stop searching
            if match_info["home_team"] and match_info["kick_off_time"]:
                break

        return match_info

    def _identify_market_types(self, market_files: Dict[str, str]) -> Dict[str, str]:
        """Identify the market type for each file"""
        market_types = {}

        for market_id, file_path in market_files.items():
            data = self.file_cache.get(file_path, [])

            for update in data:
                if "mc" not in update:
                    continue

                for market_change in update["mc"]:
                    if "marketDefinition" in market_change:
                        market_def = market_change["marketDefinition"]
                        if "marketType" in market_def:
                            market_type = market_def["marketType"]

                            # Only include markets we're interested in
                            if market_type in self.target_markets:
                                market_types[market_id] = market_type
                                logger.debug(
                                    f"Found market: {market_type} (ID: {market_id})"
                                )
                            break

                if market_id in market_types:
                    break

        logger.info(f"Identified {len(market_types)} relevant markets")
        return market_types

    def _process_markets(
        self, market_types: Dict[str, str], market_files: Dict[str, str]
    ) -> Dict[str, Dict]:
        """Process each market using appropriate processor"""
        processed_data = {}

        for market_id, market_type in market_types.items():
            if market_id not in market_files:
                continue

            processor = self.processor_factory.get_processor(market_type)
            if not processor:
                logger.warning(f"No processor for market type: {market_type}")
                continue

            try:
                data = self.file_cache[market_files[market_id]]
                if data:
                    result = processor.process(data)
                    if result and result.get("timestamps"):
                        processed_data[market_type] = result
                        logger.debug(
                            f"Processed {market_type}: {len(result['timestamps'])} data points"
                        )
            except Exception as e:
                logger.error(f"Error processing market {market_type}: {str(e)}")
                continue

        return processed_data

    def _merge_market_data(self, processed_markets: Dict[str, Dict]) -> pd.DataFrame:
        """Merge all market data into single DataFrame"""
        if not processed_markets:
            return pd.DataFrame()

        # Collect all unique timestamps
        all_timestamps = set()
        for market_data in processed_markets.values():
            if "timestamps" in market_data:
                all_timestamps.update(market_data["timestamps"])

        if not all_timestamps:
            return pd.DataFrame()

        # Create base DataFrame with all timestamps
        all_timestamps = sorted(all_timestamps)
        df = pd.DataFrame({"timestamp": all_timestamps})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        # Add data from each market
        for market_type, market_data in processed_markets.items():
            if "timestamps" not in market_data:
                continue

            # Create temporary DataFrame for this market
            market_df = pd.DataFrame(
                {"timestamp": pd.to_datetime(market_data["timestamps"], unit="ms")}
            )

            # Add all columns except timestamps
            for key, values in market_data.items():
                if key != "timestamps":
                    market_df[key] = values

            # Merge with main DataFrame
            df = pd.merge(df, market_df, on="timestamp", how="left")

        # Sort by timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)

        logger.info(f"Merged data: {len(df)} rows, {len(df.columns)} columns")
        return df

    def extract_batch(self, match_ids: List[str]) -> Dict[str, Dict]:
        """
        Extract multiple matches

        Args:
            match_ids: List of match IDs to process

        Returns:
            Dictionary mapping match_id to extracted data
        """
        results = {}

        for match_id in match_ids:
            logger.info(f"Processing match {match_id}")
            result = self.extract_match(match_id)
            if result:
                results[match_id] = result
            else:
                logger.warning(f"Failed to extract match {match_id}")

        logger.info(f"Extracted {len(results)}/{len(match_ids)} matches successfully")
        return results
