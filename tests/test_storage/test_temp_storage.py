import json
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


# @pytest.mark.skip()
def test_create():
    print()
    print("- - " * 50)
    storage = TempStorage()
    # Create folder
    try:
        folder_path = storage.create_temp_match_folder("12345")
        print(f"Created: {folder_path}")
    except ValueError as e:
        print(f"Invalid match ID: {e}")

    # Check if exists
    if storage.match_folder_exists("match_12345"):
        print("Folder exists")


@pytest.mark.skip()
def test_clean_up():
    print()
    print("- - " * 50)
    storage = TempStorage()
    # Cleanup
    success = storage.cleanup_temp_match_folder("12345")
    if success:
        print("Cleanup successful")
