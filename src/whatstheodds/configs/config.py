# src/whatstheodds/configs/config.py
"""
simple script to read the YAML of the configs/setting.yaml
the main config setting for the package.
Strongly-typed config loader for the WhatsTheOdds pipeline.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from dacite import from_dict


@dataclass
class DataBaseConfig:
    url: str


@dataclass
class BetfairClientConfig:
    env_file: str
    required_credentials: List[str]


@dataclass
class StorageConfig:
    data: str
    temp: str


@dataclass
class MappingConfig:
    mapping_dir: str
    active_files: List[str]
    tournament_country_map: Dict[str, str]


@dataclass
class BetfairRateLimitConfig:
    max_request: int
    time_window: int


@dataclass
class SearchDateComponent:
    day: int
    minute: int
    hour: int


@dataclass
class SearchEngineConfig:
    primary_strategy: str
    extended_strategy: Optional[str]
    exact_date_team_search: SearchDateComponent
    extended_strategy_conf: SearchDateComponent


@dataclass
class BetfairDownloaderConfig:
    max_auth_attempts: int
    error_file_size: int
    max_retries: int
    html_retries: int
    html_sleep_before_retry: int
    track_market_failures: bool
    skip_existing_downloads: bool
    selective_retry_enabled: bool


@dataclass
class BetfairFootballConfig:
    sport: str
    plan: str
    file_type: List[str]
    markets: List[str]


@dataclass
class ProcessingConfig:
    clean_data: bool
    pre_kickoff_mins: int
    post_kickoff_mins: int
    apply_forward_fill: bool
    resample_to_mins: bool
    clip_extreme_odds: bool
    min_odds: float
    max_odds: float
    implied_odds: bool
    odds_movements: bool


@dataclass
class ProcessorConfig:
    output_dir: str


@dataclass
class MaxworkersConfig:
    search: int
    download: int


# --- MAIN APP CONFIG ---
@dataclass
class AppConfig:
    database: DataBaseConfig
    betfair_client: BetfairClientConfig
    storage: StorageConfig
    mapping: MappingConfig
    betfair_rate_limit: BetfairRateLimitConfig
    search: SearchEngineConfig
    betfair_downloader: BetfairDownloaderConfig
    betfair_football: BetfairFootballConfig
    processing: ProcessingConfig
    processor: ProcessorConfig
    max_workers: MaxworkersConfig


# --- Config Loader ---
def load_config(config_file_name: str = "setting.yaml") -> AppConfig:
    """
    Loads the YAML file and returns a strongly-typed AppConfig object.
    """
    # Dynamically resolve the path relative to this script's location
    current_dir = Path(__file__).parent
    file = current_dir / config_file_name

    if not file.exists():
        raise FileNotFoundError(f"No config path found at: {file}")

    with open(file, "r") as f:
        raw = yaml.safe_load(f)

    return from_dict(data_class=AppConfig, data=raw)


if __name__ == "__main__":
    config = load_config()
    print("Loaded config successfully!")
    print(f"DB URL: {config.database.url}")
    print(f"Workers (Download): {config.max_workers.download}")
    print(f"Max Retries: {config.betfair_downloader.max_retries}")

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
