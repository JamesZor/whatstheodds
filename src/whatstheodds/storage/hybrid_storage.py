"""
Hybrid storage system for managing temporary and archived files
Extends TempStorage to add archiving capabilities
"""

import json
import logging
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from omegaconf import DictConfig

from whatstheodds.storage.temp_manager import TempStorage
from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


@dataclass
class ArchiveMetadata:
    """Metadata for archived matches"""

    match_id: str
    run_name: str
    archived_at: datetime
    source_path: str
    archive_path: str
    files_archived: List[str]
    total_size_bytes: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "match_id": self.match_id,
            "run_name": self.run_name,
            "archived_at": self.archived_at.isoformat(),
            "source_path": self.source_path,
            "archive_path": self.archive_path,
            "files_archived": self.files_archived,
            "total_size_bytes": self.total_size_bytes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArchiveMetadata":
        """Create from dictionary"""
        return cls(
            match_id=data["match_id"],
            run_name=data["run_name"],
            archived_at=datetime.fromisoformat(data["archived_at"]),
            source_path=data["source_path"],
            archive_path=data["archive_path"],
            files_archived=data["files_archived"],
            total_size_bytes=data["total_size_bytes"],
        )


class HybridStorage(TempStorage):
    """
    Extends TempStorage to add archiving capabilities
    Manages both temporary processing files and permanent archives
    """

    def __init__(self, cfg: Optional[DictConfig] = None):
        """
        Initialize HybridStorage

        Args:
            cfg: Configuration object with storage settings
        """
        # Initialize parent TempStorage
        super().__init__(cfg)

        # Load config if not provided
        if cfg is None:
            cfg = load_config()

        # Set archive paths from config
        self.archive_base_path = Path(
            getattr(cfg.storage, "archive_base_path", "data/archive")
        )

        # Archive settings
        self.archive_enabled = getattr(cfg.storage, "archive_enabled", True)
        self.cleanup_temp_on_archive = getattr(
            cfg.storage, "cleanup_temp_on_archive", True
        )
        self.compress_archive = getattr(cfg.storage, "compress_archive", False)

        # Create archive directory if it doesn't exist
        if self.archive_enabled:
            self.archive_base_path.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"HybridStorage initialized with archive at: {self.archive_base_path}"
            )

    def archive_match(
        self,
        match_id: str,
        run_name: str,
        cleanup_temp: Optional[bool] = None,
        overwrite: bool = False,
    ) -> Optional[Path]:
        """
        Archive match files from temp to permanent storage

        Args:
            match_id: Betfair match ID
            run_name: Processing run name
            cleanup_temp: Whether to remove temp files after archiving
            overwrite: Whether to overwrite existing archive

        Returns:
            Path to archive directory if successful, None otherwise
        """
        if not self.archive_enabled:
            logger.debug("Archiving is disabled")
            return None

        # Use config setting if cleanup_temp not specified
        if cleanup_temp is None:
            cleanup_temp = self.cleanup_temp_on_archive

        # Get source temp directory
        temp_match_dir = self.get_match_folder_path(match_id)

        if not temp_match_dir.exists():
            logger.warning(f"No temp files found for match {match_id}")
            return None

        # Get destination archive directory
        archive_match_dir = self.archive_base_path / run_name / match_id

        # Check if already archived
        if archive_match_dir.exists() and not overwrite:
            logger.info(f"Match {match_id} already archived in {run_name}")
            return None

        # Remove existing archive if overwriting
        if archive_match_dir.exists() and overwrite:
            logger.info(f"Overwriting existing archive for match {match_id}")
            shutil.rmtree(archive_match_dir)

        # Create archive directory
        archive_match_dir.mkdir(parents=True, exist_ok=True)

        # Get list of files to archive
        files_to_archive = list(temp_match_dir.glob("*.bz2"))

        if not files_to_archive:
            logger.warning(f"No .bz2 files found in {temp_match_dir}")
            return None

        # Copy files to archive
        archived_files = []
        total_size = 0

        for file_path in files_to_archive:
            dest_path = archive_match_dir / file_path.name
            try:
                shutil.copy2(file_path, dest_path)
                archived_files.append(file_path.name)
                total_size += file_path.stat().st_size
                logger.debug(f"Archived {file_path.name} to {dest_path}")
            except Exception as e:
                logger.error(f"Failed to archive {file_path}: {e}")
                # Continue with other files

        if not archived_files:
            logger.error(f"Failed to archive any files for match {match_id}")
            return None

        # Create metadata
        metadata = ArchiveMetadata(
            match_id=match_id,
            run_name=run_name,
            archived_at=datetime.now(),
            source_path=str(temp_match_dir),
            archive_path=str(archive_match_dir),
            files_archived=archived_files,
            total_size_bytes=total_size,
        )

        # Save metadata
        metadata_file = archive_match_dir / "archive_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

        logger.info(
            f"Archived {len(archived_files)} files ({total_size} bytes) "
            f"for match {match_id} to {archive_match_dir}"
        )

        # Cleanup temp files if requested
        if cleanup_temp:
            try:
                shutil.rmtree(temp_match_dir)
                logger.info(f"Cleaned up temp files for match {match_id}")
            except Exception as e:
                logger.error(f"Failed to cleanup temp files: {e}")

        return archive_match_dir

    def restore_from_archive(
        self, match_id: str, run_name: str, overwrite_temp: bool = True
    ) -> bool:
        """
        Restore match files from archive to temp storage

        Args:
            match_id: Betfair match ID
            run_name: Processing run name
            overwrite_temp: Whether to overwrite existing temp files

        Returns:
            True if successful, False otherwise
        """
        # Get archive directory
        archive_match_dir = self.archive_base_path / run_name / match_id

        if not archive_match_dir.exists():
            logger.warning(f"No archive found for match {match_id} in run {run_name}")
            return False

        # Get destination temp directory
        temp_match_dir = self.get_match_folder_path(match_id)

        # Check if temp files already exist
        if temp_match_dir.exists() and not overwrite_temp:
            logger.info(
                f"Temp files already exist for match {match_id}, skipping restore"
            )
            return False

        # Create temp directory
        temp_match_dir.mkdir(parents=True, exist_ok=True)

        # Copy archived files to temp
        archived_files = list(archive_match_dir.glob("*.bz2"))

        if not archived_files:
            logger.warning(f"No .bz2 files found in archive {archive_match_dir}")
            return False

        restored_count = 0
        for file_path in archived_files:
            dest_path = temp_match_dir / file_path.name
            try:
                shutil.copy2(file_path, dest_path)
                restored_count += 1
                logger.debug(f"Restored {file_path.name} to {dest_path}")
            except Exception as e:
                logger.error(f"Failed to restore {file_path}: {e}")

        if restored_count == 0:
            logger.error(f"Failed to restore any files for match {match_id}")
            return False

        logger.info(
            f"Restored {restored_count} files for match {match_id} "
            f"from archive to {temp_match_dir}"
        )

        return True

    def get_archive_stats(self, run_name: str) -> Dict[str, Any]:
        """
        Get statistics for an archive run

        Args:
            run_name: Processing run name

        Returns:
            Dictionary with archive statistics
        """
        archive_run_dir = self.archive_base_path / run_name

        stats = {
            "run_name": run_name,
            "archive_path": str(archive_run_dir),
            "archive_exists": archive_run_dir.exists(),
            "total_matches": 0,
            "total_files": 0,
            "total_size_bytes": 0,
            "match_ids": [],
            "earliest_archive": None,
            "latest_archive": None,
        }

        if not archive_run_dir.exists():
            return stats

        # Iterate through archived matches
        match_dirs = [d for d in archive_run_dir.iterdir() if d.is_dir()]
        stats["total_matches"] = len(match_dirs)

        earliest_date = None
        latest_date = None

        for match_dir in match_dirs:
            stats["match_ids"].append(match_dir.name)

            # Count files and size
            for file_path in match_dir.glob("*.bz2"):
                stats["total_files"] += 1
                stats["total_size_bytes"] += file_path.stat().st_size

            # Check metadata for archive date
            metadata_file = match_dir / "archive_metadata.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file, "r") as f:
                        metadata = json.load(f)

                    archived_at = datetime.fromisoformat(metadata["archived_at"])

                    if earliest_date is None or archived_at < earliest_date:
                        earliest_date = archived_at

                    if latest_date is None or archived_at > latest_date:
                        latest_date = archived_at

                except Exception as e:
                    logger.debug(f"Could not read metadata for {match_dir}: {e}")

        if earliest_date:
            stats["earliest_archive"] = earliest_date.isoformat()

        if latest_date:
            stats["latest_archive"] = latest_date.isoformat()

        # Calculate size in MB
        stats["total_size_mb"] = round(stats["total_size_bytes"] / (1024 * 1024), 2)

        return stats

    def list_archived_matches(self, run_name: str) -> List[str]:
        """
        List all archived match IDs for a run

        Args:
            run_name: Processing run name

        Returns:
            List of match IDs
        """
        archive_run_dir = self.archive_base_path / run_name

        if not archive_run_dir.exists():
            return []

        match_ids = [
            d.name
            for d in archive_run_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

        return sorted(match_ids)

    def is_archived(self, match_id: str, run_name: str) -> bool:
        """
        Check if a match is archived

        Args:
            match_id: Betfair match ID
            run_name: Processing run name

        Returns:
            True if archived, False otherwise
        """
        archive_match_dir = self.archive_base_path / run_name / match_id
        return archive_match_dir.exists()

    def get_archive_metadata(
        self, match_id: str, run_name: str
    ) -> Optional[ArchiveMetadata]:
        """
        Get metadata for an archived match

        Args:
            match_id: Betfair match ID
            run_name: Processing run name

        Returns:
            ArchiveMetadata if found, None otherwise
        """
        metadata_file = (
            self.archive_base_path / run_name / match_id / "archive_metadata.json"
        )

        if not metadata_file.exists():
            return None

        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)
            return ArchiveMetadata.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to load metadata for {match_id}: {e}")
            return None

    def cleanup_archive(self, run_name: str) -> bool:
        """
        Remove an entire archive run

        Args:
            run_name: Processing run name

        Returns:
            True if successful, False otherwise
        """
        archive_run_dir = self.archive_base_path / run_name

        if not archive_run_dir.exists():
            logger.info(f"Archive {run_name} does not exist")
            return True

        try:
            shutil.rmtree(archive_run_dir)
            logger.info(f"Removed archive for run {run_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup archive {run_name}: {e}")
            return False

    def archive_run_batch(
        self,
        match_ids: List[str],
        run_name: str,
        cleanup_temp: bool = True,
        show_progress: bool = True,
    ) -> Dict[str, bool]:
        """
        Archive multiple matches in batch

        Args:
            match_ids: List of match IDs to archive
            run_name: Processing run name
            cleanup_temp: Whether to cleanup temp files
            show_progress: Whether to show progress bar

        Returns:
            Dict mapping match_id to success status
        """
        results = {}

        if show_progress:
            try:
                from tqdm import tqdm

                match_iterator = tqdm(match_ids, desc="Archiving matches", unit="match")
            except ImportError:
                match_iterator = match_ids
        else:
            match_iterator = match_ids

        for match_id in match_iterator:
            archive_path = self.archive_match(
                match_id=match_id, run_name=run_name, cleanup_temp=cleanup_temp
            )
            results[match_id] = archive_path is not None

        # Summary
        success_count = sum(1 for success in results.values() if success)
        logger.info(
            f"Archived {success_count}/{len(match_ids)} matches for run {run_name}"
        )

        return results
