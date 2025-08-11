import betfairlightweight
import pytest
from omegaconf import DictConfig, OmegaConf

from src.betfair.api_client import CONFIG, setup_betfair_api_client


def test_basic():
    print()
    print("- - " * 50)
    print(OmegaConf.to_yaml(CONFIG))

    betfair_client: betfairlightweight.APIClient = setup_betfair_api_client()

    # print(CONFIG.betfair_client.env_file)
