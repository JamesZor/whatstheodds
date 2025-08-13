from pathlib import Path
from typing import Optional

from omegaconf import DictConfig

from src.utils import load_config


class TempStorage:

    def __init__(self, cfg: Optional[DictConfig] = None) -> None:
        self.cfg: DictConfig = load_config() if cfg is None else cfg  # type: ignore[assignment]
        self._set_up_dirs()

    def _set_up_dirs(self) -> None:
        self.dir_base: Path = Path(__file__).parent.parent.parent
        self.dir_data: Path = self.dir_base / self.cfg.storage.data
        self.dir_temp: Path = self.dir_base / self.cfg.storage.temp

    #        self.dir_run.mkdir(parents=False, exist_ok=True)

    def create_temp_file(self, match_id: str) -> None:
        """
        Creates temporary file location for data
        """

    # TODO:
    def cleanup_temp_files(self, match_id: str) -> None:
        """
        Remove temporary files after successful processing
        """
        pass
