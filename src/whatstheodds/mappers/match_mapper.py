import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from omegaconf import DictConfig

from whatstheodds.betfair.dataclasses import BetfairSearchRequest
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


class MatchMapper:
    def __init__(self, cfg: Optional[DictConfig] = None) -> None:
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]
        self.dir_mapping: Path = (
            Path(__name__).parent.parent.parent / self.cfg.mapping.mapping_dir
        )

        self._set_mappings()

    def _set_mappings(self) -> None:
        """
        Load team name mappings from JSON files

        Expected JSON format:
        {
            "team_name_source_1": "team_name_source_2",
            "another_team_source_1": "another_team_source_2"
        }
        """
        try:
            # Check if mapping directory exists
            if not self.dir_mapping.exists():
                logger.error(f"Mapping directory does not exist: {self.dir_mapping}")
                return

            # Get list of available JSON files
            available_files: List[Path] = [
                file
                for file in self.dir_mapping.iterdir()
                if file.suffix.lower() == ".json" and file.is_file()
            ]

            if not available_files:
                logger.warning(
                    f"No JSON files found in mapping directory: {self.dir_mapping}"
                )
                return

            # Filter for active files specified in config
            active_file_names = getattr(self.cfg.mapping, "active_files", [])
            if not active_file_names:
                logger.warning("No active mapping files specified in config")
                return

            active_files: List[Path] = [
                file for file in available_files if file.name in active_file_names
            ]

            if not active_files:
                logger.warning(
                    f"None of the specified active files found: {active_file_names}"
                )
                logger.info(f"Available files: {[f.name for f in available_files]}")
                return

            # Load and combine mappings from all active files
            combined_mapping: Dict[str, str] = {}
            loaded_files: List[str] = []

            for file in active_files:
                try:
                    mapping_data = self._load_json_mapping(file)
                    if mapping_data:
                        # Check for conflicts
                        conflicts = set(combined_mapping.keys()) & set(
                            mapping_data.keys()
                        )
                        if conflicts:
                            logger.warning(
                                f"Mapping conflicts in {file.name}: {conflicts}"
                            )

                        combined_mapping.update(mapping_data)
                        loaded_files.append(file.name)
                        logger.info(
                            f"Loaded {len(mapping_data)} mappings from {file.name}"
                        )

                except Exception as e:
                    logger.error(f"Failed to load mapping from {file.name}: {e}")

            self.name_map = combined_mapping
            logger.info(
                f"Successfully loaded {len(self.name_map)} total team mappings from {len(loaded_files)} files"
            )

            if loaded_files:
                logger.debug(f"Loaded files: {loaded_files}")

        except Exception as e:
            logger.error(f"Error setting up team mappings: {e}")
            self.name_map = {}

    def _load_json_mapping(self, file_path: Path) -> Dict[str, str]:
        """
        Load team name mappings from a single JSON file

        Args:
            file_path: Path to the JSON file

        Returns:
            Dict mapping source_1 team names to source_2 team names
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate that all values are strings
            if not isinstance(data, dict):
                logger.error(
                    f"Invalid JSON format in {file_path.name}: expected dict, got {type(data)}"
                )
                return {}

            # Convert all keys and values to strings and validate
            mapping = {}
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    logger.warning(
                        f"Skipping non-string mapping in {file_path.name}: {key} -> {value}"
                    )
                    continue

                # Clean up whitespace
                clean_key = key.strip()
                clean_value = value.strip()

                if clean_key and clean_value:
                    mapping[clean_key] = clean_value
                else:
                    logger.warning(
                        f"Skipping empty mapping in {file_path.name}: '{key}' -> '{value}'"
                    )

            return mapping

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path.name}: {e}")
            return {}
        except FileNotFoundError:
            logger.error(f"Mapping file not found: {file_path}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error loading {file_path.name}: {e}")
            return {}

    def get_mapped_name(self, source_1_name: str) -> Optional[str]:
        """
        Get the mapped team name for source 2

        Args:
            source_1_name: Team name from source 1

        Returns:
            Mapped team name for source 2, or None if not found
        """
        return self.name_map.get(source_1_name.strip())

    def get_reverse_mapped_name(self, source_2_name: str) -> Optional[str]:
        """
        Get the reverse mapped team name (from source 2 to source 1)

        Args:
            source_2_name: Team name from source 2

        Returns:
            Team name for source 1, or None if not found
        """
        # Create reverse mapping
        for key, value in self.name_map.items():
            if value == source_2_name.strip():
                return key
        return None

    def map_match(
        self,
        sofa_match_id: int,
        home_team_slug: str,
        away_team_slug: str,
        date: datetime,
        country_code: str = "GB",  # Default but can be overridden
    ) -> Optional[BetfairSearchRequest]:
        """
        Map a single statistical match to Betfair search parameters

        Args:
            sofa_match_id: Unique identifier for the match
            home_team_slug: Home team name from source 1
            away_team_slug: Away team name from source 1
            date: Match date
            country_code: Country code for the match (default: "GB")

        Returns:
            BetfairSearchRequest or None if mapping fails
        """
        try:
            # Validate inputs
            if not home_team_slug or not away_team_slug:
                logger.warning(
                    f"Empty team slug for match {sofa_match_id}: home='{home_team_slug}', away='{away_team_slug}'"
                )
                return None

            if not isinstance(date, datetime):
                logger.error(f"Invalid date type for match {sofa_match_id}: {type(date)}")  # type: ignore[unreachable]
                return None

            # Map team names
            home_team = self.get_mapped_name(home_team_slug)
            away_team = self.get_mapped_name(away_team_slug)

            # Check if mapping was successful
            missing_teams = []
            if home_team is None:
                missing_teams.append(f"home: '{home_team_slug}'")
            if away_team is None:
                missing_teams.append(f"away: '{away_team_slug}'")

            if missing_teams:
                logger.warning(
                    f"Failed to map teams for match {sofa_match_id}: {', '.join(missing_teams)}"
                )
                return None

            # Create and return the search request
            search_request = BetfairSearchRequest(
                sofa_match_id=sofa_match_id,
                home=home_team,  # type: ignore[arg-type]
                away=away_team,  # type: ignore[arg-type]
                date=date,
                country=country_code,
            )

            logger.debug(
                f"Successfully mapped match {sofa_match_id}: {home_team_slug} -> {home_team} vs {away_team_slug} -> {away_team}"
            )
            return search_request

        except Exception as e:
            logger.error(f"Unexpected error mapping match {sofa_match_id}: {e}")
            return None

    def map_match_with_tournament(
        self,
        sofa_match_id: int,
        home_team_slug: str,
        away_team_slug: str,
        date: datetime,
        tournament_id: Optional[int] = None,
    ) -> Optional[BetfairSearchRequest]:
        """
        Map a match with tournament-based country detection

        Args:
            sofa_match_id: Unique identifier for the match
            home_team_slug: Home team name from source 1
            away_team_slug: Away team name from source 1
            date: Match date
            tournament_id: Tournament ID to determine country

        Returns:
            BetfairSearchRequest or None if mapping fails
        """
        # Get country from tournament ID
        country_code = (
            self._get_country_from_tournament(tournament_id) if tournament_id else "GB"
        )

        return self.map_match(
            sofa_match_id=sofa_match_id,
            home_team_slug=home_team_slug,
            away_team_slug=away_team_slug,
            date=date,
            country_code=country_code,
        )

    # HACK: - add attr to init
    def _get_country_from_tournament(self, tournament_id: int) -> str:
        """
        Map tournament ID to country code

        Args:
            tournament_id: Tournament identifier

        Returns:
            Country code string
        """
        # You could load this from config or a separate mapping file
        tournament_country_map = getattr(self.cfg.mapping, "tournament_country_map", {})
        # Try config first, then default, then fallback to GB
        country = tournament_country_map.get(str(tournament_id))
        logger.debug(f"Mapped tournament {tournament_id} to country {country}")
        return country  # type: ignore[return-value]

    def map_match_from_row(
        self,
        row: pd.Series,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team_slug",
        away_column: str = "away_team_slug",
        tournament_column: str = "tournament_id",
    ) -> Optional[BetfairSearchRequest]:
        """
        Map a pandas DataFrame row to BetfairSearchRequest

        Args:
            row: Pandas Series (DataFrame row)
            date_column: Name of the date column (default: "match_date")
            match_id_column: Name of match ID column (default: "match_id")
            home_column: Name of home team column (default: "home_team_slug")
            away_column: Name of away team column (default: "away_team_slug")
            tournament_column: Name of tournament ID column (default: "tournament_id")

        Returns:
            BetfairSearchRequest or None if mapping fails
        """
        try:
            # Extract values from row
            sofa_match_id = row[match_id_column]
            home_team_slug = row[home_column]
            away_team_slug = row[away_column]
            match_date = row[date_column]
            tournament_id = row.get(
                tournament_column
            )  # Use .get() in case column doesn't exist

            # TODO: pd.datetime is not the same as datetime :(
            # Convert date to datetime if needed
            if isinstance(match_date, str):
                match_date = pd.to_datetime(match_date)
            elif not isinstance(match_date, datetime):
                # Handle other date types (numpy datetime64, etc.)
                match_date = pd.to_datetime(match_date)

            # Use existing method with tournament support
            return self.map_match_with_tournament(
                sofa_match_id=sofa_match_id,
                home_team_slug=home_team_slug,
                away_team_slug=away_team_slug,
                date=match_date,
                tournament_id=tournament_id,
            )

        except KeyError as e:
            logger.error(f"Missing required column in row: {e}")
            return None
        except Exception as e:
            logger.error(f"Error mapping row to search request: {e}")
            return None
