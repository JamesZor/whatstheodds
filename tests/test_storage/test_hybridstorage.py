"""
Tests for HybridStorage system with archiving capabilities
"""

import bz2
import json
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from whatstheodds.storage.hybrid_storage import ArchiveMetadata, HybridStorage


@pytest.fixture
def temp_base_dir(tmp_path):
    """Create temporary base directory structure"""
    base_dir = tmp_path / "storage_test"
    base_dir.mkdir()

    # Create temp and archive subdirectories
    (base_dir / "temp").mkdir()
    (base_dir / "archive").mkdir()

    return base_dir


@pytest.fixture
def mock_config():
    """Create mock configuration"""
    config = Mock()
    config.storage = Mock()
    config.storage.temp_base_path = "temp"
    config.storage.archive_base_path = "archive"
    config.storage.cleanup_temp_on_archive = True
    config.storage.archive_enabled = True
    config.storage.compress_archive = False
    return config


@pytest.fixture
def hybrid_storage(temp_base_dir, mock_config):
    """Create HybridStorage instance"""
    # Update config with actual paths
    mock_config.storage.temp_base_path = str(temp_base_dir / "temp")
    mock_config.storage.archive_base_path = str(temp_base_dir / "archive")

    return HybridStorage(cfg=mock_config)


def create_mock_bz2_file(filepath: Path, content: dict = None):
    """Create a mock compressed file"""
    if content is None:
        content = {"test": "data", "timestamp": datetime.now().isoformat()}

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with bz2.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(content, f)

    return filepath


def create_mock_match_files(base_dir: Path, match_id: str, markets: list = None):
    """Create mock match files in a directory"""
    if markets is None:
        markets = ["match_odds", "over_under", "correct_score"]

    match_dir = base_dir / match_id
    match_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for market in markets:
        file_path = create_mock_bz2_file(
            match_dir / f"{market}.bz2", {"market": market, "match_id": match_id}
        )
        files.append(file_path)

    return files


class TestArchiveMetadata:
    """Tests for ArchiveMetadata dataclass"""

    def test_metadata_creation(self):
        """Test creating archive metadata"""
        metadata = ArchiveMetadata(
            match_id="31679991",
            run_name="test_run",
            archived_at=datetime.now(),
            source_path="/temp/31679991",
            archive_path="/archive/test_run/31679991",
            files_archived=["match_odds.bz2", "over_under.bz2"],
            total_size_bytes=2048,
        )

        assert metadata.match_id == "31679991"
        assert metadata.run_name == "test_run"
        assert len(metadata.files_archived) == 2
        assert metadata.total_size_bytes == 2048

    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary"""
        metadata = ArchiveMetadata(
            match_id="31679991",
            run_name="test_run",
            archived_at=datetime.now(),
            source_path="/temp/31679991",
            archive_path="/archive/test_run/31679991",
            files_archived=["match_odds.bz2"],
            total_size_bytes=1024,
        )

        data = metadata.to_dict()

        assert data["match_id"] == "31679991"
        assert data["run_name"] == "test_run"
        assert "archived_at" in data
        assert isinstance(data["files_archived"], list)

    def test_metadata_from_dict(self):
        """Test creating metadata from dictionary"""
        data = {
            "match_id": "31679991",
            "run_name": "test_run",
            "archived_at": datetime.now().isoformat(),
            "source_path": "/temp/31679991",
            "archive_path": "/archive/test_run/31679991",
            "files_archived": ["match_odds.bz2"],
            "total_size_bytes": 1024,
        }

        metadata = ArchiveMetadata.from_dict(data)

        assert metadata.match_id == "31679991"
        assert isinstance(metadata.archived_at, datetime)


class TestHybridStorage:
    """Tests for HybridStorage class"""

    def test_initialization(self, hybrid_storage, temp_base_dir):
        """Test HybridStorage initialization"""
        assert hybrid_storage.temp_base_path == Path(temp_base_dir / "temp")
        assert hybrid_storage.archive_base_path == Path(temp_base_dir / "archive")
        assert hybrid_storage.archive_enabled is True

    def test_archive_match_success(self, hybrid_storage, temp_base_dir):
        """Test successful match archiving"""
        match_id = "31679991"
        run_name = "test_run"

        # Create temp files to archive
        temp_files = create_mock_match_files(
            temp_base_dir / "temp", match_id, ["match_odds", "over_under"]
        )

        # Archive the match
        archive_path = hybrid_storage.archive_match(match_id, run_name)

        assert archive_path is not None
        assert archive_path.exists()
        assert archive_path == temp_base_dir / "archive" / run_name / match_id

        # Check archived files exist
        archived_files = list(archive_path.glob("*.bz2"))
        assert len(archived_files) == 2

        # Check metadata file created
        metadata_file = archive_path / "archive_metadata.json"
        assert metadata_file.exists()

        # Verify metadata content
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        assert metadata["match_id"] == match_id
        assert metadata["run_name"] == run_name
        assert len(metadata["files_archived"]) == 2

    def test_archive_match_with_cleanup(self, hybrid_storage, temp_base_dir):
        """Test archiving with temp cleanup"""
        match_id = "31679991"
        run_name = "test_run"

        # Create temp files
        temp_match_dir = temp_base_dir / "temp" / match_id
        temp_files = create_mock_match_files(temp_base_dir / "temp", match_id)

        # Archive with cleanup
        archive_path = hybrid_storage.archive_match(
            match_id, run_name, cleanup_temp=True
        )

        assert archive_path is not None

        # Check temp files were cleaned up
        assert not temp_match_dir.exists()

        # Check archive files exist
        assert archive_path.exists()
        archived_files = list(archive_path.glob("*.bz2"))
        assert len(archived_files) == 3

    def test_archive_match_no_temp_files(self, hybrid_storage):
        """Test archiving when no temp files exist"""
        archive_path = hybrid_storage.archive_match("nonexistent", "test_run")

        assert archive_path is None

    def test_archive_match_already_archived(self, hybrid_storage, temp_base_dir):
        """Test archiving when already archived"""
        match_id = "31679991"
        run_name = "test_run"

        # Create existing archive
        archive_dir = temp_base_dir / "archive" / run_name / match_id
        archive_dir.mkdir(parents=True)
        create_mock_bz2_file(archive_dir / "existing.bz2")

        # Create new temp files
        create_mock_match_files(temp_base_dir / "temp", match_id, ["new_market"])

        # Try to archive
        archive_path = hybrid_storage.archive_match(match_id, run_name, overwrite=False)

        # Should skip due to existing archive
        assert archive_path is None

    def test_archive_match_overwrite(self, hybrid_storage, temp_base_dir):
        """Test archiving with overwrite"""
        match_id = "31679991"
        run_name = "test_run"

        # Create existing archive
        archive_dir = temp_base_dir / "archive" / run_name / match_id
        create_mock_match_files(
            temp_base_dir / "archive" / run_name, match_id, ["old_market"]
        )

        # Create new temp files
        create_mock_match_files(temp_base_dir / "temp", match_id, ["new_market"])

        # Archive with overwrite
        archive_path = hybrid_storage.archive_match(match_id, run_name, overwrite=True)

        assert archive_path is not None

        # Check old files are gone, new files exist
        archived_files = list(archive_path.glob("*.bz2"))
        assert len(archived_files) == 1
        assert "new_market.bz2" in [f.name for f in archived_files]

    def test_restore_from_archive_success(self, hybrid_storage, temp_base_dir):
        """Test successful restoration from archive"""
        match_id = "31679991"
        run_name = "test_run"

        # Create archived files
        archive_files = create_mock_match_files(
            temp_base_dir / "archive" / run_name, match_id, ["match_odds", "over_under"]
        )

        # Create metadata
        metadata = ArchiveMetadata(
            match_id=match_id,
            run_name=run_name,
            archived_at=datetime.now(),
            source_path=str(temp_base_dir / "temp" / match_id),
            archive_path=str(temp_base_dir / "archive" / run_name / match_id),
            files_archived=["match_odds.bz2", "over_under.bz2"],
            total_size_bytes=2048,
        )

        metadata_file = (
            temp_base_dir / "archive" / run_name / match_id / "archive_metadata.json"
        )
        with open(metadata_file, "w") as f:
            json.dump(metadata.to_dict(), f)

        # Restore from archive
        success = hybrid_storage.restore_from_archive(match_id, run_name)

        assert success is True

        # Check temp files exist
        temp_dir = temp_base_dir / "temp" / match_id
        assert temp_dir.exists()

        restored_files = list(temp_dir.glob("*.bz2"))
        assert len(restored_files) == 2

        # Verify content matches
        for restored_file in restored_files:
            with bz2.open(restored_file, "rt") as f:
                data = json.load(f)
                assert data["match_id"] == match_id

    def test_restore_from_archive_not_found(self, hybrid_storage):
        """Test restoration when archive doesn't exist"""
        success = hybrid_storage.restore_from_archive("nonexistent", "test_run")

        assert success is False

    def test_restore_with_existing_temp(self, hybrid_storage, temp_base_dir):
        """Test restoration when temp files already exist"""
        match_id = "31679991"
        run_name = "test_run"

        # Create archive
        create_mock_match_files(
            temp_base_dir / "archive" / run_name, match_id, ["archived_market"]
        )

        # Create existing temp files
        create_mock_match_files(temp_base_dir / "temp", match_id, ["temp_market"])

        # Restore without overwrite
        success = hybrid_storage.restore_from_archive(
            match_id, run_name, overwrite_temp=False
        )

        assert success is False

        # Existing temp files should remain
        temp_files = list((temp_base_dir / "temp" / match_id).glob("*.bz2"))
        assert len(temp_files) == 1
        assert "temp_market.bz2" in [f.name for f in temp_files]

    def test_get_archive_stats(self, hybrid_storage, temp_base_dir):
        """Test getting archive statistics"""
        run_name = "test_run"

        # Create multiple archived matches
        for i in range(3):
            match_id = f"match_{i}"
            archive_dir = temp_base_dir / "archive" / run_name / match_id
            create_mock_match_files(
                temp_base_dir / "archive" / run_name, match_id, ["market1", "market2"]
            )

            # Add metadata
            metadata = ArchiveMetadata(
                match_id=match_id,
                run_name=run_name,
                archived_at=datetime.now(),
                source_path=f"/temp/{match_id}",
                archive_path=str(archive_dir),
                files_archived=["market1.bz2", "market2.bz2"],
                total_size_bytes=1024 * (i + 1),
            )

            with open(archive_dir / "archive_metadata.json", "w") as f:
                json.dump(metadata.to_dict(), f)

        # Get statistics
        stats = hybrid_storage.get_archive_stats(run_name)

        assert stats["run_name"] == run_name
        assert stats["total_matches"] == 3
        assert stats["total_files"] == 6  # 3 matches * 2 files each
        assert stats["total_size_bytes"] > 0
        assert len(stats["match_ids"]) == 3
        assert stats["archive_exists"] is True

    def test_get_archive_stats_empty(self, hybrid_storage):
        """Test getting stats for non-existent archive"""
        stats = hybrid_storage.get_archive_stats("nonexistent_run")

        assert stats["run_name"] == "nonexistent_run"
        assert stats["total_matches"] == 0
        assert stats["total_files"] == 0
        assert stats["total_size_bytes"] == 0
        assert stats["archive_exists"] is False

    def test_list_archived_matches(self, hybrid_storage, temp_base_dir):
        """Test listing archived matches"""
        run_name = "test_run"

        # Create archived matches
        match_ids = ["match_1", "match_2", "match_3"]
        for match_id in match_ids:
            create_mock_match_files(temp_base_dir / "archive" / run_name, match_id)

        # List matches
        archived = hybrid_storage.list_archived_matches(run_name)

        assert len(archived) == 3
        assert set(archived) == set(match_ids)

    def test_is_archived(self, hybrid_storage, temp_base_dir):
        """Test checking if match is archived"""
        run_name = "test_run"
        match_id = "31679991"

        # Check before archiving
        assert hybrid_storage.is_archived(match_id, run_name) is False

        # Create archive
        create_mock_match_files(temp_base_dir / "archive" / run_name, match_id)

        # Check after archiving
        assert hybrid_storage.is_archived(match_id, run_name) is True

    def test_get_archive_metadata(self, hybrid_storage, temp_base_dir):
        """Test getting archive metadata"""
        run_name = "test_run"
        match_id = "31679991"

        # Create archive with metadata
        archive_dir = temp_base_dir / "archive" / run_name / match_id
        archive_dir.mkdir(parents=True)

        metadata = ArchiveMetadata(
            match_id=match_id,
            run_name=run_name,
            archived_at=datetime.now(),
            source_path="/temp/path",
            archive_path=str(archive_dir),
            files_archived=["file1.bz2"],
            total_size_bytes=1024,
        )

        with open(archive_dir / "archive_metadata.json", "w") as f:
            json.dump(metadata.to_dict(), f)

        # Get metadata
        retrieved = hybrid_storage.get_archive_metadata(match_id, run_name)

        assert retrieved is not None
        assert retrieved.match_id == match_id
        assert retrieved.total_size_bytes == 1024

    def test_cleanup_archive(self, hybrid_storage, temp_base_dir):
        """Test cleaning up archive"""
        run_name = "test_run"

        # Create archive
        for i in range(3):
            create_mock_match_files(temp_base_dir / "archive" / run_name, f"match_{i}")

        archive_path = temp_base_dir / "archive" / run_name
        assert archive_path.exists()

        # Cleanup
        success = hybrid_storage.cleanup_archive(run_name)

        assert success is True
        assert not archive_path.exists()

    def test_archive_disabled(self, hybrid_storage, mock_config):
        """Test behavior when archiving is disabled"""
        mock_config.storage.archive_enabled = False
        hybrid_storage.archive_enabled = False

        result = hybrid_storage.archive_match("31679991", "test_run")

        assert result is None
