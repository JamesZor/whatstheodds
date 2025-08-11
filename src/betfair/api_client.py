import logging
from pathlib import Path
from typing import Dict, List

import betfairlightweight  # type: ignore
from dotenv import dotenv_values
from omegaconf import DictConfig

from src.utils import load_config

logger = logging.getLogger(__name__)


##############################
# Utils
##############################


# set config
CONFIG: DictConfig = load_config()  # type: ignore[assignment]
DIR: Path = Path(__file__).parent.parent


def _valid_betfair_api_config(api_config: Dict[str, str | None]) -> None:
    """
    Validates required credentials are present and non-empty

    Args (dict): api_config
    """

    # Check for missing credentials
    missing_credentials: List[str] = [
        cred
        for cred in CONFIG.betfair_client.required_credentials
        if cred not in api_config
    ]

    if missing_credentials:
        missing_credentials_msg: str = (
            f"Missing required credentials in env :{', '.join(missing_credentials)}"
        )
        logger.error(missing_credentials_msg)
        raise ValueError(missing_credentials_msg)

    # check for empty credentials
    empty_credentials: List[str] = [
        cred
        for cred in CONFIG.betfair_client.required_credentials
        if not api_config.get(cred, "").strip()  # type: ignore[union-attr]
    ]
    if empty_credentials:
        empty_credentials_msg: str = (
            f"Credentials cannot be empty: {', '.join(empty_credentials)}"
        )
        logger.error(empty_credentials_msg)
        raise ValueError(empty_credentials_msg)


##############################
#
##############################


def get_credentials_from_env(
    env_file_name: str = CONFIG.betfair_client.env_file,
) -> Dict[str, str]:
    logger.debug(f"ENV file @ {env_file_name}")
    env_file = DIR / env_file_name
    logger.debug(f"ENV file @ {env_file}")
    if not env_file.is_file():
        logger.error(f"Could not find .env file: {env_file =}.")
        raise FileExistsError("Env file can not be found.")

    try:

        client_details: Dict[str, str | None] = dotenv_values(env_file)
        _valid_betfair_api_config(api_config=client_details)
        return client_details  # type: ignore[return-value]

    except Exception as e:
        logger.error(f"Could not get env file: {str(e)}.")
        raise e


def setup_betfair_api_client() -> betfairlightweight.APIClient:
    """
    Initialize the betfairlightweight trader API client and verify connection.

    Returns:
        betfairlightweight.APIClient: Initialized and connected API client

    Raises:
        ConnectionError: If unable to establish connection with the API
        ValueError: If API configuration is invalid
    """

    credentials = get_credentials_from_env()

    try:
        # Create trading instance with validated credentials
        betfair = betfairlightweight.APIClient(
            username=credentials["USER_NAME"],
            password=credentials["PASSWORD"],
            app_key=credentials["APP_KEY"],
        )

        # Verify connection by attempting to login
        # This ensures we have valid credentials and can connect
        # trading.login()
        betfair.login_interactive()

        logger.info("Successfully connected to Betfair API")
        return betfair

    except betfairlightweight.exceptions.BetfairError as e:
        # Log the error and raise a more descriptive exception
        logger.error(f"Failed to connect to Betfair API: {str(e)}")
        raise ConnectionError(
            f"Failed to establish connection with Betfair API: {str(e)}"
        )
