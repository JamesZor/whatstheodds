import logging

import pytest
from omegaconf import DictConfig, OmegaConf

from src.storage.temp_manager import TempStorage


def test_basic_setup():
    print()
    print("- - " * 50)

    t = TempStorage()

    print(OmegaConf.to_yaml(t.cfg))

    print(t.dir_base)
    print(t.dir_data)
    print(t.dir_temp)
