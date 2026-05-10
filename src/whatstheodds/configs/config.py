# src/whatstheodds/configs/config.py
"""
simple script to read the YAML of the configs/setting.yaml
the main config setting for the package.
"""

from dataclasses import dataclass
from pathlib import Path

import yaml
from dacite import from_dict


@dataclass
class DataBaseConfig:
    url: str


@dataclass
class BetfairClientConfig:
    env_file: str
    required_credentials: list[str]


@dataclass
class SearchDateComponent:
    day: int
    minute: int
    hour: int


@dataclass
class SearchEngineConfig:
    primary_strategy: str
    extended_strategy: str
    exact_date_team_search: SearchDateComponent
    extended_strategy_conf: SearchDateComponent


@dataclass
class BetfairFootballConfig:
    sport: str
    plan: str
    file_type: list[str]
    markets: list[str]


@dataclass
class MaxworkersConfig:
    search: int
    download: int


# main app config
@dataclass
class AppConfig:
    database: DataBaseConfig
    betfair_client: BetfairClientConfig
    search: SearchEngineConfig
    betfair_football: BetfairFootballConfig
    max_workers: MaxworkersConfig


# --- Configs Functions

CONFIG_FOLDER: str = "configs"


def load_config(config_file_name: str = "settings.yaml") -> AppConfig:
    """
    Loads the YAML file and returns a strongly-type AppCOnfig obeject
    """

    # file: Path = Path(__file__).parent.parent / CONFIG_FOLDER / config_file_name
    file: Path = Path(
        "/home/james/bet_project/whatstheodds/src/whatstheodds/configs/setting.yaml"
    )

    if not file.exists():
        raise FileExistsError(f"No config path found: {file=}.")

    with open(file, "r") as f:
        raw = yaml.safe_load(f)

    return from_dict(data_class=AppConfig, data=raw)


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    config = load_config()
    print(f"Loaded config successfully!")
    print(f"DB URL: {config.database.url}")
    print(
        f"Betfair Client: {config.betfairclient.env_file} : {config.betfairclient.required_credentials}"
    )
    print(
        f"search: ps:{config.search.primary_strategy}, ss:{config.search.extended_strategy}, : {config.search.exact_date_team_search}"
    )
