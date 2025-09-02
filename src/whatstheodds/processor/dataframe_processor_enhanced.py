"""
Enhanced DataFrameProcessor methods with state management and user features
improves the DataFrameProcessor
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from omegaconf import DictConfig
from tqdm import tqdm

from whatstheodds.pipeline.pipeline_coordinator import (
    ProcessingResult,
    ProcessingStatus,
)
from whatstheodds.pipeline.pipeline_coordinator_enhanced import PipelineCoordinator
from whatstheodds.processor.dataframe_processor import (
    ProcessedDataFrames,
    ResultsStorage,
)
from whatstheodds.processor.state_manager import ProcessingStateManager
from whatstheodds.storage.hybrid_storage import HybridStorage
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


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

    # chunk 5 adds

    def process_and_save_with_state(
        self,
        df: pd.DataFrame,
        run_name: str,
        output_dir: Optional[Path] = None,
        save_formats: List[str] = ["csv"],
        compression: Optional[str] = None,
        archive_successful: bool = True,
        cleanup_archived: bool = True,
        save_state_frequency: int = 10,
        **processing_kwargs,
    ) -> Tuple[ProcessedDataFrames, Dict[str, Path], ProcessingStateManager]:
        """
        Process DataFrame with state tracking and automatic saving

        Args:
            df: Input DataFrame with match information
            run_name: Name for this processing run
            output_dir: Output directory (default from config)
            save_formats: Formats to save (csv, parquet, pickle)
            compression: Compression to use
            archive_successful: Archive successful matches
            cleanup_archived: Cleanup temp after archiving
            save_state_frequency: Save state every N matches
            **processing_kwargs: Additional arguments for processing

        Returns:
            Tuple of (ProcessedDataFrames, saved_files_dict, state_manager)
        """
        logger.info(f"Starting processing run: {run_name}")
        start_time = datetime.now()

        # Setup output directory
        if output_dir is None:
            output_dir = Path(self.cfg.processor.output_dir) / run_name
        else:
            output_dir = Path(output_dir) / run_name

        output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize state manager
        state_mgr = ProcessingStateManager(output_dir, run_name)
        logger.info(f"State tracking initialized: {state_mgr.state_file}")

        # Initialize hybrid storage if archiving is enabled
        if archive_successful and not hasattr(self, "hybrid_storage"):
            self.hybrid_storage = HybridStorage(cfg=self.cfg)

        # Process with state tracking
        logger.info(f"Processing {len(df)} matches...")

        results = self.coordinator.process_dataframe_with_state(
            df=df,
            state_manager=state_mgr,
            archive_on_success=archive_successful,
            cleanup_on_success=cleanup_archived,
            save_state_frequency=save_state_frequency,
            **processing_kwargs,
        )

        # Build output DataFrames
        processed_data = self._build_processed_dataframes(results)

        # Archive successful matches if enabled
        if archive_successful:
            self._archive_successful_from_state(state_mgr)

        # Save final state
        state_mgr.save_state()
        logger.info(f"Final state saved: {state_mgr.state_file}")

        # Save output files
        saved_files = self.storage.save_processed_dataframes(
            processed_data=processed_data,
            run_name=run_name,
            save_formats=save_formats,
            compression=compression,
        )

        # Log summary
        processing_time = (datetime.now() - start_time).total_seconds()
        stats = state_mgr.get_statistics()

        logger.info(
            f"Processing complete: {stats['fully_successful_matches']}/{stats['total_matches']} "
            f"successful in {processing_time:.1f}s"
        )

        return processed_data, saved_files, state_mgr

    def retry_failed_from_state(
        self,
        state_file_path: Path,
        max_retries: Optional[int] = None,
        archive_successful: bool = True,
        update_existing_files: bool = True,
    ) -> Tuple[List[ProcessingResult], ProcessedDataFrames]:
        """
        Retry failed matches using saved state file

        Args:
            state_file_path: Path to state JSON file
            max_retries: Maximum matches to retry (None = all)
            archive_successful: Archive newly successful matches
            update_existing_files: Update existing output files

        Returns:
            Tuple of (retry_results, updated_processed_data)
        """
        logger.info(f"Loading state from: {state_file_path}")

        # Load state
        state_mgr = ProcessingStateManager(
            state_file_path.parent, state_file_path.stem.replace("_state", "")
        )
        state_mgr.load_state()

        # Check for retry candidates
        retry_candidates = state_mgr.get_retry_candidates()

        if not retry_candidates:
            logger.info("No matches need retry")
            return [], ProcessedDataFrames(
                join_table=pd.DataFrame(),
                odds_table=pd.DataFrame(),
                details_table=pd.DataFrame(),
                processing_summary={},
            )

        logger.info(f"Found {len(retry_candidates)} matches needing retry")

        # Perform retry
        retry_results = self.coordinator.retry_failed_matches(
            state_manager=state_mgr,
            max_retries=max_retries,
            archive_on_success=archive_successful,
        )

        # Build DataFrames from retry results
        successful_retries = [r for r in retry_results if r.is_success]

        if successful_retries:
            retry_data = self._build_processed_dataframes(successful_retries)

            # Update existing files if requested
            if update_existing_files:
                run_dir = state_file_path.parent
                updated_data = self.load_and_update_results(run_dir, retry_data)

                # Save updated files
                self.storage.save_processed_dataframes(
                    processed_data=updated_data,
                    run_name=state_mgr.run_name,
                    save_formats=["csv"],
                )

                logger.info(
                    f"Updated output files with {len(successful_retries)} retried matches"
                )
                return retry_results, updated_data
            else:
                return retry_results, retry_data
        else:
            logger.warning("No successful retries")
            return retry_results, ProcessedDataFrames(
                join_table=pd.DataFrame(),
                odds_table=pd.DataFrame(),
                details_table=pd.DataFrame(),
                processing_summary={},
            )

    def generate_failure_report(
        self, state_file_path: Path, output_format: str = "dict"
    ) -> Dict[str, Any]:
        """
        Generate comprehensive failure report from state

        Args:
            state_file_path: Path to state JSON file
            output_format: Format for report (dict, json, markdown)

        Returns:
            Dict with failure analysis and recommendations
        """
        # Load state
        state_mgr = ProcessingStateManager(
            state_file_path.parent, state_file_path.stem.replace("_state", "")
        )
        state_mgr.load_state()

        # Get base report from coordinator
        base_report = self.coordinator.generate_retry_report(state_manager=state_mgr)

        # Enhance with additional analysis
        stats = state_mgr.get_statistics()

        # Analyze failure patterns
        failure_patterns = self._analyze_failure_patterns(state_mgr)

        # Generate recommendations
        recommendations = self._generate_recommendations(state_mgr, failure_patterns)

        # Build comprehensive report
        report = {
            "run_name": state_mgr.run_name,
            "report_generated": datetime.now().isoformat(),
            "summary": {
                "total_matches": stats["total_matches"],
                "successful": stats["fully_successful_matches"],
                "with_failures": stats["matches_with_failures"],
                "not_found": len(
                    [s for s in state_mgr.processing_state.values() if s.search_error]
                ),
                "success_rate": (
                    stats["fully_successful_matches"] / stats["total_matches"] * 100
                    if stats["total_matches"] > 0
                    else 0
                ),
            },
            "failure_breakdown": base_report.get("failure_by_market", {}),
            "retry_status": {
                "can_retry": len(base_report.get("retry_candidates", [])),
                "max_attempts_reached": len(
                    base_report.get("max_attempts_reached", [])
                ),
                "total_attempts": stats["total_download_attempts"],
            },
            "failure_patterns": failure_patterns,
            "recommendations": recommendations,
            "has_failures": stats["matches_with_failures"] > 0,
            "state_file": str(state_file_path),
        }

        # Format output
        if output_format == "json":
            return json.dumps(report, indent=2, default=str)
        elif output_format == "markdown":
            return self._format_report_as_markdown(report)
        else:
            return report

    def process_with_auto_retry(
        self,
        df: pd.DataFrame,
        run_name: str,
        output_dir: Optional[Path] = None,
        max_retry_attempts: int = 2,
        archive_successful: bool = True,
        cleanup_archived: bool = True,
        **processing_kwargs,
    ) -> Tuple[
        ProcessedDataFrames,
        Dict[str, Path],
        ProcessingStateManager,
        List[ProcessingResult],
    ]:
        """
        Process DataFrame with automatic retry of failures

        Args:
            df: Input DataFrame
            run_name: Processing run name
            output_dir: Output directory
            max_retry_attempts: Maximum retry attempts
            archive_successful: Archive successful matches
            cleanup_archived: Cleanup after archiving
            **processing_kwargs: Additional processing arguments

        Returns:
            Tuple of (processed_data, saved_files, state_manager, retry_results)
        """
        # Initial processing
        processed_data, saved_files, state_mgr = self.process_and_save_with_state(
            df=df,
            run_name=run_name,
            output_dir=output_dir,
            archive_successful=False,  # Don't archive yet
            **processing_kwargs,
        )

        # Automatic retry
        all_retry_results = []

        for attempt in range(max_retry_attempts):
            retry_candidates = state_mgr.get_retry_candidates()

            if not retry_candidates:
                logger.info("No more matches need retry")
                break

            logger.info(
                f"Retry attempt {attempt + 1}/{max_retry_attempts}: {len(retry_candidates)} matches"
            )

            retry_results = self.coordinator.retry_failed_matches(
                state_manager=state_mgr,
                archive_on_success=False,  # Archive all at the end
            )

            all_retry_results.extend(retry_results)

            # Update processed data with retry results
            successful_retries = [r for r in retry_results if r.is_success]
            if successful_retries:
                retry_data = self._build_processed_dataframes(successful_retries)
                processed_data = self._merge_processed_data(processed_data, retry_data)

        # Archive all successful matches
        if archive_successful:
            self._archive_successful_from_state(state_mgr)

        # Save final state and updated files
        state_mgr.save_state()

        saved_files = self.storage.save_processed_dataframes(
            processed_data=processed_data, run_name=run_name, save_formats=["csv"]
        )

        # Log final summary
        stats = state_mgr.get_statistics()
        logger.info(
            f"Processing complete with auto-retry: "
            f"{stats['fully_successful_matches']}/{stats['total_matches']} successful"
        )

        return processed_data, saved_files, state_mgr, all_retry_results

    def load_and_update_results(
        self, existing_run_dir: Path, new_data: ProcessedDataFrames
    ) -> ProcessedDataFrames:
        """
        Load existing results and update with new data

        Args:
            existing_run_dir: Directory with existing output files
            new_data: New data to merge

        Returns:
            Updated ProcessedDataFrames
        """
        # Load existing files
        existing_join = pd.DataFrame()
        existing_odds = pd.DataFrame()
        existing_details = pd.DataFrame()

        join_file = existing_run_dir / "join_table.csv"
        if join_file.exists():
            existing_join = pd.read_csv(join_file)

        odds_file = existing_run_dir / "odds_table.csv"
        if odds_file.exists():
            existing_odds = pd.read_csv(odds_file)

        details_file = existing_run_dir / "details_table.csv"
        if details_file.exists():
            existing_details = pd.read_csv(details_file)

        # Merge with new data
        updated_join = pd.concat(
            [existing_join, new_data.join_table], ignore_index=True
        )
        updated_odds = pd.concat(
            [existing_odds, new_data.odds_table], ignore_index=True
        )
        updated_details = pd.concat(
            [existing_details, new_data.details_table], ignore_index=True
        )

        # Remove duplicates
        updated_join = updated_join.drop_duplicates(subset=["sofa_match_id"])
        updated_odds = updated_odds.drop_duplicates()
        updated_details = updated_details.drop_duplicates(subset=["sofa_match_id"])

        return ProcessedDataFrames(
            join_table=updated_join,
            odds_table=updated_odds,
            details_table=updated_details,
            processing_summary={
                "total_matches": len(updated_join),
                "total_odds_records": len(updated_odds),
                "updated_at": datetime.now().isoformat(),
            },
        )

    def get_processing_status(self, state_file_path: Path) -> Dict[str, Any]:
        """
        Get quick processing status summary

        Args:
            state_file_path: Path to state file

        Returns:
            Dict with status summary
        """
        # Load state
        state_mgr = ProcessingStateManager(
            state_file_path.parent, state_file_path.stem.replace("_state", "")
        )
        state_mgr.load_state()

        stats = state_mgr.get_statistics()

        total = stats["total_matches"]
        successful = stats["fully_successful_matches"]

        status = {
            "run_name": state_mgr.run_name,
            "total_matches": total,
            "successful": successful,
            "failed": total - successful,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "can_retry": stats["retry_candidates"],
            "at_max_attempts": len(
                [
                    s
                    for s in state_mgr.processing_state.values()
                    if s.download_attempts >= state_mgr.max_download_attempts
                ]
            ),
            "estimated_retry_time": f"{stats['retry_candidates'] * 30}s",
            "state_file": str(state_file_path),
        }

        return status

    def archive_successful_matches(
        self, state_file_path: Path, cleanup_temp: bool = True
    ) -> Dict[str, Any]:
        """
        Archive all successful matches from state

        Args:
            state_file_path: Path to state file
            cleanup_temp: Cleanup temp files after archiving

        Returns:
            Dict with archive results
        """
        # Load state
        state_mgr = ProcessingStateManager(
            state_file_path.parent, state_file_path.stem.replace("_state", "")
        )
        state_mgr.load_state()

        # Initialize hybrid storage
        if not hasattr(self, "hybrid_storage"):
            self.hybrid_storage = HybridStorage(cfg=self.cfg)

        # Get successful matches not yet archived
        to_archive = []
        already_archived = []

        for sofa_id, state in state_mgr.processing_state.items():
            if state.is_fully_successful() and state.betfair_id:
                if not state.archive_path:
                    to_archive.append(state.betfair_id)
                else:
                    already_archived.append(state.betfair_id)

        logger.info(f"Archiving {len(to_archive)} successful matches...")

        if to_archive:
            # Batch archive
            archive_results = self.hybrid_storage.archive_run_batch(
                match_ids=to_archive,
                run_name=state_mgr.run_name,
                cleanup_temp=cleanup_temp,
                show_progress=True,
            )

            # Update state with archive paths
            for match_id, success in archive_results.items():
                if success:
                    # Find sofa_id for this betfair_id
                    for sofa_id, state in state_mgr.processing_state.items():
                        if state.betfair_id == match_id:
                            archive_path = (
                                self.hybrid_storage.archive_base_path
                                / state_mgr.run_name
                                / match_id
                            )
                            state_mgr.update_archive_path(sofa_id, str(archive_path))
                            break

            state_mgr.save_state()

            archived_count = sum(1 for s in archive_results.values() if s)
            logger.info(f"Archived {archived_count} matches")
        else:
            archive_results = {}
            archived_count = 0

        return {
            "archived_count": archived_count,
            "already_archived_count": len(already_archived),
            "failed_count": len([s for s in archive_results.values() if not s]),
            "archive_results": archive_results,
        }

    def cleanup_temp_files(
        self,
        match_ids: Optional[List[str]] = None,
        state_file_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """
        Cleanup temporary files

        Args:
            match_ids: Specific match IDs to cleanup
            state_file_path: State file to get match IDs from

        Returns:
            Dict with cleanup results
        """
        if not hasattr(self, "hybrid_storage"):
            self.hybrid_storage = HybridStorage(cfg=self.cfg)

        # Get match IDs from state if not provided
        if match_ids is None and state_file_path:
            state_mgr = ProcessingStateManager(
                state_file_path.parent, state_file_path.stem.replace("_state", "")
            )
            state_mgr.load_state()

            # Get archived matches
            match_ids = [
                state.betfair_id
                for state in state_mgr.processing_state.values()
                if state.archive_path and state.betfair_id
            ]

        if not match_ids:
            logger.warning("No match IDs provided for cleanup")
            return {"cleaned_count": 0, "failed_count": 0}

        cleaned = 0
        failed = 0

        for match_id in match_ids:
            try:
                success = self.hybrid_storage.cleanup_temp_match_folder(match_id)
                if success:
                    cleaned += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Failed to cleanup {match_id}: {e}")
                failed += 1

        logger.info(f"Cleaned up {cleaned} temp folders")

        return {"cleaned_count": cleaned, "failed_count": failed}

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate configuration for all features

        Returns:
            Dict with validation results
        """
        validation: Dict = {
            "state_tracking_enabled": getattr(
                self.cfg.processor, "enable_state_tracking", False
            ),
            "archive_enabled": getattr(self.cfg.processor.archive, "enabled", False),
            "has_output_dir": hasattr(self.cfg.processor, "output_dir"),
            "has_all_components": all(
                [
                    hasattr(self, "coordinator"),
                    hasattr(self, "storage"),
                ]
            ),
            "warnings": [],
            "errors": [],
        }

        # Check for warnings
        if not validation["state_tracking_enabled"]:
            validation["warnings"].append("State tracking is disabled")

        if not validation["archive_enabled"]:
            validation["warnings"].append("Archiving is disabled")

        # Check for errors
        if not validation["has_all_components"]:
            validation["errors"].append("Missing required components")

        validation["is_valid"] = len(validation["errors"]) == 0

        return validation

    # Helper methods (private)

    def _build_processed_dataframes(
        self, results: List[ProcessingResult]
    ) -> ProcessedDataFrames:
        """Build ProcessedDataFrames from processing results"""
        successful_results = [r for r in results if r.is_success]

        join_table = self._build_join_table(successful_results)
        odds_table = self._build_odds_table(successful_results)
        details_table = self._build_details_table(successful_results)

        return ProcessedDataFrames(
            join_table=join_table,
            odds_table=odds_table,
            details_table=details_table,
            processing_summary=self._create_processing_summary(
                results, datetime.now(), datetime.now()
            ),
        )

    def _merge_processed_data(
        self, existing: ProcessedDataFrames, new: ProcessedDataFrames
    ) -> ProcessedDataFrames:
        """Merge two ProcessedDataFrames objects"""
        return ProcessedDataFrames(
            join_table=pd.concat(
                [existing.join_table, new.join_table], ignore_index=True
            ),
            odds_table=pd.concat(
                [existing.odds_table, new.odds_table], ignore_index=True
            ),
            details_table=pd.concat(
                [existing.details_table, new.details_table], ignore_index=True
            ),
            processing_summary={
                **existing.processing_summary,
                **new.processing_summary,
            },
        )

    def _archive_successful_from_state(self, state_mgr: ProcessingStateManager) -> None:
        """Archive successful matches from state"""
        if not hasattr(self, "hybrid_storage"):
            self.hybrid_storage = HybridStorage(cfg=self.cfg)

        to_archive = [
            state.betfair_id
            for state in state_mgr.processing_state.values()
            if state.is_fully_successful()
            and state.betfair_id
            and not state.archive_path
        ]

        if to_archive:
            logger.info(f"Archiving {len(to_archive)} successful matches...")
            archive_results = self.hybrid_storage.archive_run_batch(
                match_ids=to_archive,
                run_name=state_mgr.run_name,
                cleanup_temp=True,
                show_progress=False,
            )

            # Update state with archive paths
            for match_id, success in archive_results.items():
                if success:
                    for sofa_id, state in state_mgr.processing_state.items():
                        if state.betfair_id == match_id:
                            archive_path = (
                                self.hybrid_storage.archive_base_path
                                / state_mgr.run_name
                                / match_id
                            )
                            state_mgr.update_archive_path(sofa_id, str(archive_path))
                            break

    def _analyze_failure_patterns(
        self, state_mgr: ProcessingStateManager
    ) -> Dict[str, Any]:
        """Analyze patterns in failures"""
        patterns = {
            "most_failed_market": None,
            "failure_rate_by_market": {},
            "common_error_messages": {},
            "time_based_patterns": {},
        }

        # Count failures by market
        market_failures = {}
        for state in state_mgr.processing_state.values():
            for market, status in state.markets.items():
                if status == "failed":
                    market_failures[market] = market_failures.get(market, 0) + 1

        if market_failures:
            patterns["most_failed_market"] = max(
                market_failures, key=market_failures.get
            )
            patterns["failure_rate_by_market"] = market_failures

        return patterns

    def _generate_recommendations(
        self, state_mgr: ProcessingStateManager, failure_patterns: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations based on failures"""
        recommendations = []

        stats = state_mgr.get_statistics()

        if stats["retry_candidates"] > 0:
            recommendations.append(
                f"Retry {stats['retry_candidates']} failed matches using: "
                "retry_failed_from_state()"
            )

        if failure_patterns.get("most_failed_market"):
            recommendations.append(
                f"Market '{failure_patterns['most_failed_market']}' has the most failures - "
                "check API availability"
            )

        if stats["failed_searches"] > 0:
            recommendations.append(
                f"{stats['failed_searches']} matches not found - "
                "verify team name mappings"
            )

        return recommendations

    def _format_report_as_markdown(self, report: Dict[str, Any]) -> str:
        """Format report as markdown"""
        md = f"# Processing Report: {report['run_name']}\n\n"
        md += f"Generated: {report['report_generated']}\n\n"

        md += "## Summary\n"
        for key, value in report["summary"].items():
            md += f"- **{key}**: {value}\n"

        md += "\n## Recommendations\n"
        for rec in report["recommendations"]:
            md += f"- {rec}\n"

        return md

    # old

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
