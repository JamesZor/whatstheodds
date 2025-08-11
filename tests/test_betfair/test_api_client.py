import pytest
from omegaconf import DictConfig, OmegaConf

from src.betfair.api_client import CONFIG


def test_basic():
    print()
    print("- - " * 50)
    print(OmegaConf.to_yaml(CONFIG))
