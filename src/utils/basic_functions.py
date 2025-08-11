import logging
from pathlib import Path
from typing import Dict, Optional

from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


def load_config(
    config_dir: str = "configs", config_file: str = "default.yaml"
) -> Optional[DictConfig]:
    file: Path = Path(__file__).parent.parent / config_dir / config_file
    if file.is_file():
        logger.info(f"Config file found {file =}.")
        return OmegaConf.load(file)  # type: ignore[return-value]
    logger.error(f"No config path found: {file = }.")
