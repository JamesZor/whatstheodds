# extractors/odds_processor.py
"""
Post-processing and cleaning of extracted odds data
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import numpy as np
import pandas as pd
from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class OddsDataProcessor:
    """Clean and prepare odds data for analysis"""

    def __init__(self, config: DictConfig):
        """Initialize with configuration"""
        self.config = config

        # Processing settings
        self.pre_kickoff_mins = config.get("processing", {}).get("pre_kickoff_mins", 10)
        self.post_kickoff_mins = config.get("processing", {}).get(
            "post_kickoff_mins", 100
        )
        self.apply_forward_fill = config.get("processing", {}).get(
            "apply_forward_fill", False
        )

        self.resample_to_mins = config.get("processing", {}).get(
            "resample_to_mins", True
        )
        self.clip_extreme_odds = config.get("processing", {}).get(
            "clip_extreme_odds", True
        )
        self.min_odds = config.get("processing", {}).get("min_odds", 1.01)
        self.max_odds = config.get("processing", {}).get("max_odds", 1000)

    def clean_odds_data(
        self, odds_df: pd.DataFrame, kickoff_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Clean and prepare odds data

        Args:
            odds_df: Raw odds DataFrame
            kickoff_time: Match kickoff time (ISO format string)

        Returns:
            Cleaned DataFrame with additional features
        """
        if odds_df.empty:
            return odds_df

        df = odds_df.copy()

        # Ensure timestamp column is datetime
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Forward fill missing values
        if self.apply_forward_fill:
            df = df.ffill()

        # Clip extreme odds if configured
        if self.clip_extreme_odds:
            df = self._clip_odds_values(df)

        # Add time-based features if kickoff time provided
        if kickoff_time:
            df = self._add_time_features(df, kickoff_time)
            df = self._filter_by_time_window(df, kickoff_time)

            if self.resample_to_mins:
                df = self._resample_to_minutes(df, kickoff_time)

        # Calculate derived features
        df = self._calculate_derived_features(df)

        return df

    def _clip_odds_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clip extreme odds values to reasonable range"""
        odds_columns = [
            col
            for col in df.columns
            if col not in ["timestamp", "minutes", "seconds_to_kickoff"]
        ]

        for col in odds_columns:
            if col in df.columns:
                df[col] = df[col].clip(lower=self.min_odds, upper=self.max_odds)

        return df

    def _add_time_features(self, df: pd.DataFrame, kickoff_time: str) -> pd.DataFrame:
        """Add time-based features relative to kickoff"""
        kickoff = pd.to_datetime(kickoff_time)

        # Remove timezone if present
        if kickoff.tzinfo is not None:
            kickoff = kickoff.tz_localize(None)

        # Calculate time differences
        # df["seconds_to_kickoff"] = (kickoff - df["timestamp"]).dt.total_seconds()
        df["minutes"] = (
            -(kickoff - df["timestamp"]).dt.total_seconds() / 60
        )  # Negative before, positive after kickoff
        df["minutes"] = df["minutes"].round().astype(int)

        return df

    def _filter_by_time_window(
        self, df: pd.DataFrame, kickoff_time: str
    ) -> pd.DataFrame:
        """Filter data to relevant time window around kickoff"""
        kickoff = pd.to_datetime(kickoff_time)

        if kickoff.tzinfo is not None:
            kickoff = kickoff.tz_localize(None)

        start_time = kickoff - timedelta(minutes=self.pre_kickoff_mins)
        end_time = kickoff + timedelta(minutes=self.post_kickoff_mins)

        filtered = df[
            (df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)
        ].copy()

        # If no data in window, take last available data before kickoff
        if filtered.empty and not df.empty:
            pre_kickoff = df[df["timestamp"] < kickoff]
            if not pre_kickoff.empty:
                filtered = pre_kickoff.tail(10).copy()

        return filtered

    def _resample_to_minutes(self, df: pd.DataFrame, kickoff_time: str) -> pd.DataFrame:
        """Resample data to consistent minute intervals"""
        if "minutes" not in df.columns or df.empty:
            return df

        kickoff = pd.to_datetime(kickoff_time)
        if kickoff.tzinfo is not None:
            kickoff = kickoff.tz_localize(None)

        # Create minute range
        min_minute = int(df["minutes"].min())
        max_minute = int(df["minutes"].max())

        # Create template with all minutes
        minute_range = pd.DataFrame({"minutes": range(min_minute, max_minute + 1)})

        # Generate timestamps for each minute
        minute_range["timestamp"] = minute_range["minutes"].apply(
            lambda m: kickoff + timedelta(minutes=m)
        )

        # Get the last value for each minute
        df_grouped = df.groupby("minutes").last().reset_index()

        # Merge and forward fill
        resampled = pd.merge(
            minute_range, df_grouped, on="minutes", how="left", suffixes=("", "_old")
        )

        # Use generated timestamps
        resampled = resampled.drop(columns=["timestamp_old"], errors="ignore")

        # Forward fill missing values
        resampled = resampled.ffill()

        return resampled

    def _calculate_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate additional features from odds"""

        # Calculate implied probabilities for match odds
        if self.config.get("processing", {}).get("implied_odds", False):
            if all(col in df.columns for col in ["home", "away", "draw"]):
                df["home_prob"] = 1 / df["home"]
                df["away_prob"] = 1 / df["away"]
                df["draw_prob"] = 1 / df["draw"]

                # Calculate overround (bookmaker margin)
                df["overround"] = df["home_prob"] + df["away_prob"] + df["draw_prob"]

                # Normalize probabilities
                df["home_prob_norm"] = df["home_prob"] / df["overround"]
                df["away_prob_norm"] = df["away_prob"] / df["overround"]
                df["draw_prob_norm"] = df["draw_prob"] / df["overround"]

        # Calculate odds movements
        if self.config.get("processing", {}).get("odds_movements", False):
            odds_cols = [
                col
                for col in df.columns
                if col not in ["timestamp", "minutes", "seconds_to_kickoff"]
            ]

            for col in odds_cols:
                if (
                    col in df.columns
                    and not col.endswith("_prob")
                    and not col.endswith("_norm")
                ):
                    # Calculate percentage change
                    df[f"{col}_pct_change"] = df[col].pct_change() * 100

                    # Calculate absolute change
                    df[f"{col}_abs_change"] = df[col].diff()

        return df

    def get_pre_match_odds(self, df: pd.DataFrame) -> Dict[str, float]:
        """Extract pre-match (closing) odds"""
        if df.empty:
            return {}

        # Get last row before kickoff (minute 0)
        if "minutes" in df.columns:
            pre_match = df[df["minutes"] <= 0]
            if not pre_match.empty:
                last_row = pre_match.iloc[-1]
            else:
                last_row = df.iloc[0]
        else:
            last_row = df.iloc[-1]

        odds = {}
        for col in [
            "home",
            "away",
            "draw",
            "over_0_5",
            "under_0_5",
            "over_1_5",
            "under_1_5",
            "over_2_5",
            "under_2_5",
        ]:
            if col in last_row:
                odds[f"pre_match_{col}"] = last_row[col]

        return odds
