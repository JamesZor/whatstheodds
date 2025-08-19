import logging
import shutil
from pathlib import Path
from typing import Optional

from omegaconf import DictConfig

from whatstheodds.utils import load_config

logger = logging.getLogger(__name__)


class TempStorage:
    def __init__(self, cfg: Optional[DictConfig] = None) -> None:
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]
        self._set_up_dirs()

    def _set_up_dirs(self) -> None:
        """Set up base directories for data and temp storage"""
        try:
            self.dir_base: Path = Path(__file__).parent.parent.parent.parent
            self.dir_data: Path = self.dir_base / self.cfg.storage.data
            self.dir_temp: Path = self.dir_base / self.cfg.storage.temp

            # Create directories with better error handling
            self.dir_data.mkdir(parents=True, exist_ok=True)
            self.dir_temp.mkdir(parents=True, exist_ok=True)

            logger.info(
                f"Storage directories initialized: data={self.dir_data}, temp={self.dir_temp}"
            )

        except KeyError as e:
            raise ValueError(f"Missing storage configuration in config: {e}")
        except PermissionError as e:
            raise PermissionError(
                f"Permission denied creating storage directories: {e}"
            )
        except OSError as e:
            raise OSError(f"Failed to create storage directories: {e}")

    def _validate_match_id(self, match_id: str) -> None:
        """Validate match_id format and safety"""
        if not match_id or not isinstance(match_id, str):
            raise ValueError("match_id must be a non-empty string")

        if not match_id.strip():
            raise ValueError("match_id cannot be whitespace only")

        # Check for dangerous characters that could cause path issues
        dangerous_chars = ["/", "\\", "..", "<", ">", ":", '"', "|", "?", "*"]
        if any(char in match_id for char in dangerous_chars):
            raise ValueError(f"match_id contains invalid characters: {match_id}")

    def get_match_folder_path(self, match_id: str) -> Path:
        """Get the path to match folder"""
        self._validate_match_id(match_id)
        return self.dir_temp / match_id

    def match_folder_exists(self, match_id: str) -> bool:
        """Check if match folder exists"""
        try:
            match_folder = self.get_match_folder_path(match_id)
            return match_folder.exists() and match_folder.is_dir()
        except ValueError:
            return False

    def create_temp_match_folder(self, match_id: str) -> Path:
        """
        Creates temporary file location for data

        Args:
            match_id: Unique identifier for the match

        Returns:
            Path to the created match folder

        Raises:
            ValueError: If match_id is invalid
            PermissionError: If unable to create folder due to permissions
            OSError: If folder creation fails for other reasons
        """
        self._validate_match_id(match_id)

        try:
            match_folder: Path = self.dir_temp / match_id
            match_folder.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created temp match folder: {match_folder}")
            return match_folder

        except PermissionError as e:
            error_msg = f"Permission denied creating match folder for {match_id}: {e}"
            logger.error(error_msg)
            raise PermissionError(error_msg)
        except OSError as e:
            error_msg = f"Failed to create match folder for {match_id}: {e}"
            logger.error(error_msg)
            raise OSError(error_msg)

    def cleanup_temp_match_folder(self, match_id: str, force: bool = False) -> bool:
        """
        Remove temporary files after successful processing

        Args:
            match_id: Unique identifier for the match
            force: If True, ignore some safety checks

        Returns:
            True if cleanup successful, False otherwise

        Raises:
            ValueError: If match_id is invalid
        """
        self._validate_match_id(match_id)

        try:
            match_folder: Path = self.dir_temp / match_id

            # Check if folder exists
            if not match_folder.exists():
                logger.warning(f"Match folder does not exist: {match_folder}")
                return True  # Already gone, so cleanup successful

            # Safety check - ensure it's actually in our temp directory
            if not force and not self._is_safe_to_delete(match_folder):
                logger.error(
                    f"Refusing to delete folder outside temp directory: {match_folder}"
                )
                return False

            # Remove the folder and all contents
            shutil.rmtree(match_folder)
            logger.info(f"Successfully cleaned up temp match folder: {match_folder}")
            return True

        except PermissionError as e:
            logger.error(f"Permission denied cleaning up match folder {match_id}: {e}")
            return False
        except OSError as e:
            logger.error(f"Failed to cleanup match folder {match_id}: {e}")
            return False

    def _is_safe_to_delete(self, path: Path) -> bool:
        """Check if path is safe to delete (within our temp directory)"""
        try:
            # Resolve to absolute paths to avoid symlink tricks
            resolved_path = path.resolve()
            resolved_temp = self.dir_temp.resolve()

            # Check if the path is within our temp directory
            return resolved_path.is_relative_to(resolved_temp)
        except (OSError, ValueError):
            return False

    def cleanup_all_temp_folders(self) -> int:
        """
        Clean up all temporary match folders

        Returns:
            Number of folders successfully cleaned up
        """
        cleaned_count = 0

        try:
            if not self.dir_temp.exists():
                logger.info("Temp directory doesn't exist, nothing to clean")
                return 0

            for match_folder in self.dir_temp.iterdir():
                if match_folder.is_dir():
                    try:
                        shutil.rmtree(match_folder)
                        cleaned_count += 1
                        logger.debug(f"Cleaned up: {match_folder}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup {match_folder}: {e}")

            logger.info(f"Cleaned up {cleaned_count} temp folders")
            return cleaned_count

        except Exception as e:
            logger.error(f"Error during bulk cleanup: {e}")
            return cleaned_count

    def get_temp_folder_size(self, match_id: str) -> int:
        """
        Get total size of temp folder in bytes

        Returns:
            Size in bytes, or 0 if folder doesn't exist
        """
        try:
            match_folder = self.get_match_folder_path(match_id)
            if not match_folder.exists():
                return 0

            total_size = 0
            for file_path in match_folder.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size

            return total_size

        except Exception as e:
            logger.warning(f"Failed to calculate folder size for {match_id}: {e}")
            return 0

    def list_temp_folders(self) -> list[str]:
        """
        List all existing temp match folders

        Returns:
            List of match_ids that have temp folders
        """
        try:
            if not self.dir_temp.exists():
                return []

            return [
                folder.name for folder in self.dir_temp.iterdir() if folder.is_dir()
            ]
        except Exception as e:
            logger.error(f"Failed to list temp folders: {e}")
            return []
