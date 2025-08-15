import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from omegaconf import DictConfig

from src.pipeline.pipeline_coordinator import (
    PipelineCoordinator,
    ProcessingResult,
    ProcessingStatus,
)
from src.utils import load_config

logger = logging.getLogger(__name__)


@dataclass
class ProcessedDataFrames:
    """Container for the three output DataFrames"""

    join_table: pd.DataFrame
    odds_table: pd.DataFrame
    details_table: pd.DataFrame
    processing_summary: Dict[str, Any]

    @property
    def total_matches(self) -> int:
        """Total number of matches in join table"""
        return len(self.join_table)

    @property
    def has_odds_data(self) -> bool:
        """Check if any odds data was extracted"""
        return len(self.odds_table) > 0

    @property
    def has_details_data(self) -> bool:
        """Check if any match details were extracted"""
        return len(self.details_table) > 0


class ResultsStorage:
    """Handles saving processed DataFrames to various formats"""

    def __init__(
        self,
        base_output_dir: Union[str, Path] = "output",
        cfg: Optional[DictConfig] = None,
    ):
        """
        Initialize storage manager

        Args:
            base_output_dir: Base directory for saving files
            cfg: Configuration object
        """
        self.cfg = cfg or load_config()
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"ResultsStorage initialized with output dir: {self.base_output_dir}"
        )

    def save_processed_dataframes(
        self,
        processed_data: ProcessedDataFrames,
        run_name: Optional[str] = None,
        include_timestamp: bool = True,
        save_formats: List[str] = ["csv"],
        compression: Optional[str] = None,
    ) -> Dict[str, Path]:
        """
        Save the three DataFrames to files

        Args:
            processed_data: ProcessedDataFrames object
            run_name: Optional name for this processing run
            include_timestamp: Whether to include timestamp in filenames
            save_formats: List of formats to save (csv, parquet, pickle)
            compression: Compression to use (gzip, bz2, etc.)

        Returns:
            Dict mapping table name to saved file path
        """
        # Create run directory
        run_dir = self._create_run_directory(run_name, include_timestamp)

        saved_files = {}

        # Define the DataFrames to save
        dataframes_to_save = {
            "join_table": processed_data.join_table,
            "odds_table": processed_data.odds_table,
            "details_table": processed_data.details_table,
        }

        # Save each DataFrame in each requested format
        for format_type in save_formats:
            for table_name, df in dataframes_to_save.items():
                if not df.empty:
                    file_path = self._save_dataframe(
                        df=df,
                        table_name=table_name,
                        run_dir=run_dir,
                        format_type=format_type,
                        compression=compression,
                    )
                    saved_files[f"{table_name}_{format_type}"] = file_path
                else:
                    logger.warning(f"Skipping empty DataFrame: {table_name}")

        # Save processing summary
        summary_path = self._save_processing_summary(
            processed_data.processing_summary, run_dir
        )
        saved_files["processing_summary"] = summary_path

        # Save metadata about the run
        metadata_path = self._save_run_metadata(processed_data, run_dir, saved_files)
        saved_files["run_metadata"] = metadata_path

        logger.info(f"Saved {len(saved_files)} files to {run_dir}")
        return saved_files

    def _create_run_directory(
        self, run_name: Optional[str], include_timestamp: bool
    ) -> Path:
        """Create directory for this processing run"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if run_name:
            if include_timestamp:
                dir_name = f"{run_name}_{timestamp}"
            else:
                dir_name = run_name
        else:
            dir_name = f"processing_run_{timestamp}"

        run_dir = self.base_output_dir / dir_name
        run_dir.mkdir(parents=True, exist_ok=True)

        return run_dir

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        run_dir: Path,
        format_type: str,
        compression: Optional[str],
    ) -> Path:
        """Save a single DataFrame to file"""

        # Determine file extension
        if format_type == "csv":
            extension = "csv"
        elif format_type == "parquet":
            extension = "parquet"
        elif format_type == "pickle":
            extension = "pkl"
        else:
            raise ValueError(f"Unsupported format: {format_type}")

        # Add compression extension if specified
        if compression:
            filename = f"{table_name}.{extension}.{compression}"
        else:
            filename = f"{table_name}.{extension}"

        file_path = run_dir / filename

        # Save DataFrame
        try:
            if format_type == "csv":
                df.to_csv(file_path, index=False, compression=compression)
            elif format_type == "parquet":
                df.to_parquet(file_path, compression=compression)
            elif format_type == "pickle":
                df.to_pickle(file_path, compression=compression)

            logger.debug(f"Saved {table_name} ({len(df)} rows) to {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to save {table_name} to {file_path}: {str(e)}")
            raise

    def _save_processing_summary(self, summary: Dict[str, Any], run_dir: Path) -> Path:
        """Save processing summary as JSON"""
        import json

        summary_path = run_dir / "processing_summary.json"

        # Make summary JSON serializable
        json_summary = {}
        for key, value in summary.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                json_summary[key] = value.isoformat()
            else:
                json_summary[key] = value

        with open(summary_path, "w") as f:
            json.dump(json_summary, f, indent=2, default=str)

        return summary_path

    def _save_run_metadata(
        self,
        processed_data: ProcessedDataFrames,
        run_dir: Path,
        saved_files: Dict[str, Path],
    ) -> Path:
        """Save metadata about this run"""
        import json

        metadata = {
            "run_timestamp": datetime.now().isoformat(),
            "total_matches_processed": processed_data.total_matches,
            "odds_data_rows": len(processed_data.odds_table),
            "details_data_rows": len(processed_data.details_table),
            "has_odds_data": processed_data.has_odds_data,
            "has_details_data": processed_data.has_details_data,
            "saved_files": {name: str(path) for name, path in saved_files.items()},
            "dataframe_shapes": {
                "join_table": processed_data.join_table.shape,
                "odds_table": processed_data.odds_table.shape,
                "details_table": processed_data.details_table.shape,
            },
        }

        metadata_path = run_dir / "run_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return metadata_path


class DataFrameProcessor:
    """Processes DataFrames through the pipeline and creates structured output"""

    def __init__(
        self,
        coordinator: Optional[PipelineCoordinator] = None,
        storage: Optional[ResultsStorage] = None,
        cfg: Optional[DictConfig] = None,
    ):
        """
        Initialize the DataFrame processor

        Args:
            coordinator: PipelineCoordinator instance (will create if None)
            storage: ResultsStorage instance (will create if None)
            cfg: Configuration object
        """
        self.cfg = cfg or load_config()
        self.coordinator = coordinator or PipelineCoordinator(cfg=self.cfg)
        self.storage = storage or ResultsStorage(cfg=self.cfg)

        logger.info("DataFrameProcessor initialized")

    def process_dataframe_to_tables(
        self,
        df: pd.DataFrame,
        date_column: str = "match_date",
        match_id_column: str = "match_id",
        home_column: str = "home_team_slug",
        away_column: str = "away_team_slug",
        tournament_column: str = "tournament_id",
        cleanup_on_success: bool = True,
        cleanup_on_failure: bool = False,
    ) -> ProcessedDataFrames:
        """
        Process DataFrame and return three structured tables

        Args:
            df: Input DataFrame with match information
            date_column: Name of the date column
            match_id_column: Name of match ID column
            home_column: Name of home team column
            away_column: Name of away team column
            tournament_column: Name of tournament ID column
            cleanup_on_success: Whether to cleanup temp files after success
            cleanup_on_failure: Whether to cleanup temp files after failure

        Returns:
            ProcessedDataFrames containing join_table, odds_table, details_table
        """
        logger.info(f"Starting DataFrame processing for {len(df)} matches")
        start_time = datetime.now()

        # Process all matches through the pipeline
        results = self.coordinator.process_dataframe(
            df=df,
            date_column=date_column,
            match_id_column=match_id_column,
            home_column=home_column,
            away_column=away_column,
            tournament_column=tournament_column,
            cleanup_on_success=cleanup_on_success,
            cleanup_on_failure=cleanup_on_failure,
        )

        # Filter successful results
        successful_results = [r for r in results if r.is_success]

        logger.info(
            f"Pipeline completed: {len(successful_results)}/{len(results)} successful"
        )

        # Build the three DataFrames
        join_table = self._build_join_table(successful_results)
        odds_table = self._build_odds_table(successful_results)
        details_table = self._build_details_table(successful_results)

        # Create processing summary
        processing_summary = self._create_processing_summary(
            results, start_time, datetime.now()
        )

        processed_data = ProcessedDataFrames(
            join_table=join_table,
            odds_table=odds_table,
            details_table=details_table,
            processing_summary=processing_summary,
        )

        logger.info(
            f"Created DataFrames: join({len(join_table)}), "
            f"odds({len(odds_table)}), details({len(details_table)})"
        )

        return processed_data

    def _build_join_table(self, results: List[ProcessingResult]) -> pd.DataFrame:
        """Build join table with sofa_match_id and betfair_match_id"""
        join_data = []

        for result in results:
            if result.betfair_match_id:
                join_data.append(
                    {
                        "sofa_match_id": result.sofa_match_id,
                        "betfair_match_id": result.betfair_match_id,
                    }
                )

        if join_data:
            join_df = pd.DataFrame(join_data)
            # Ensure proper data types
            join_df["sofa_match_id"] = join_df["sofa_match_id"].astype(int)
            join_df["betfair_match_id"] = join_df["betfair_match_id"].astype(str)
            return join_df
        else:
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=["sofa_match_id", "betfair_match_id"])

    def _build_odds_table(self, results: List[ProcessingResult]) -> pd.DataFrame:
        """Build combined odds table from all successful extractions"""
        all_odds_data = []

        for result in results:
            if result.has_odds_data:
                odds_df = result.extracted_data["odds_data"].copy()

                # Add identifier columns
                odds_df["sofa_match_id"] = result.sofa_match_id
                odds_df["betfair_match_id"] = result.betfair_match_id

                # Reorder columns to put IDs first
                id_columns = ["sofa_match_id", "betfair_match_id"]
                other_columns = [
                    col for col in odds_df.columns if col not in id_columns
                ]
                odds_df = odds_df[id_columns + other_columns]

                all_odds_data.append(odds_df)

        if all_odds_data:
            # Concatenate all odds data
            combined_odds = pd.concat(all_odds_data, ignore_index=True, sort=False)

            # Ensure consistent data types for ID columns
            combined_odds["sofa_match_id"] = combined_odds["sofa_match_id"].astype(int)
            combined_odds["betfair_match_id"] = combined_odds[
                "betfair_match_id"
            ].astype(str)

            logger.info(
                f"Combined odds data: {len(combined_odds)} rows from {len(all_odds_data)} matches"
            )
            return combined_odds
        else:
            # Return empty DataFrame with ID columns
            return pd.DataFrame(columns=["sofa_match_id", "betfair_match_id"])

    def _build_details_table(self, results: List[ProcessingResult]) -> pd.DataFrame:
        """Build details table with match information"""
        details_data = []

        for result in results:
            if result.extracted_data and "match_info" in result.extracted_data:
                match_info = result.extracted_data["match_info"]

                # Extract details
                details_row = {
                    "sofa_match_id": result.sofa_match_id,
                    "betfair_match_id": result.betfair_match_id,
                    "home_team": match_info.get("home_team"),
                    "away_team": match_info.get("away_team"),
                    "kick_off_time": match_info.get("kick_off_time"),
                    "event_name": match_info.get("event_name"),
                    "competition": match_info.get("competition"),
                    "extraction_time": result.extracted_data.get("extraction_time"),
                    "markets_processed": ",".join(
                        result.extracted_data.get("markets_processed", [])
                    ),
                    "processing_time_seconds": result.processing_time_seconds,
                }

                details_data.append(details_row)

        if details_data:
            details_df = pd.DataFrame(details_data)

            # Ensure proper data types
            details_df["sofa_match_id"] = details_df["sofa_match_id"].astype(int)
            details_df["betfair_match_id"] = details_df["betfair_match_id"].astype(str)

            # Convert timestamps
            if "kick_off_time" in details_df.columns:
                details_df["kick_off_time"] = pd.to_datetime(
                    details_df["kick_off_time"], errors="coerce"
                )
            if "extraction_time" in details_df.columns:
                details_df["extraction_time"] = pd.to_datetime(
                    details_df["extraction_time"], errors="coerce"
                )

            return details_df
        else:
            # Return empty DataFrame with correct columns
            return pd.DataFrame(
                columns=[
                    "sofa_match_id",
                    "betfair_match_id",
                    "home_team",
                    "away_team",
                    "kick_off_time",
                    "event_name",
                    "competition",
                    "extraction_time",
                    "markets_processed",
                    "processing_time_seconds",
                ]
            )

    def _create_processing_summary(
        self, results: List[ProcessingResult], start_time: datetime, end_time: datetime
    ) -> Dict[str, Any]:
        """Create summary of processing results"""
        total_matches = len(results)
        successful = len([r for r in results if r.is_success])
        failed = total_matches - successful

        # Count failure types
        failure_counts = {}
        for result in results:
            if not result.is_success:
                status = result.status.value
                failure_counts[status] = failure_counts.get(status, 0) + 1

        # Processing time statistics
        processing_times = [
            r.processing_time_seconds
            for r in results
            if r.processing_time_seconds is not None
        ]

        summary = {
            "total_matches": total_matches,
            "successful_matches": successful,
            "failed_matches": failed,
            "success_rate_percent": round(
                (successful / total_matches * 100) if total_matches > 0 else 0, 2
            ),
            "failure_breakdown": failure_counts,
            "total_processing_time_seconds": (end_time - start_time).total_seconds(),
            "start_time": start_time,
            "end_time": end_time,
        }

        if processing_times:
            summary.update(
                {
                    "avg_processing_time_per_match": sum(processing_times)
                    / len(processing_times),
                    "min_processing_time": min(processing_times),
                    "max_processing_time": max(processing_times),
                }
            )

        return summary

    def process_and_save(
        self,
        df: pd.DataFrame,
        run_name: Optional[str] = None,
        save_formats: List[str] = ["csv"],
        compression: Optional[str] = None,
        **processing_kwargs,
    ) -> Tuple[ProcessedDataFrames, Dict[str, Path]]:
        """
        Process DataFrame and automatically save results

        Args:
            df: Input DataFrame
            run_name: Name for this processing run
            save_formats: Formats to save (csv, parquet, pickle)
            compression: Compression to use
            **processing_kwargs: Arguments passed to process_dataframe_to_tables

        Returns:
            Tuple of (ProcessedDataFrames, saved_files_dict)
        """
        # Process the DataFrame
        processed_data = self.process_dataframe_to_tables(df, **processing_kwargs)

        # Save the results
        saved_files = self.storage.save_processed_dataframes(
            processed_data=processed_data,
            run_name=run_name,
            save_formats=save_formats,
            compression=compression,
        )

        return processed_data, saved_files
