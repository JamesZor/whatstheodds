from datetime import datetime
from pathlib import Path

import pandas as pd

from whatstheodds.betfair.downloader import BetfairDownloader, BetfairDownloadError
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.extractors import MatchExtractor
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor import DataFrameProcessor

### configs
MATCHES_FILE_PATH = Path(
    "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
)
ODDS_FILE_PATH = Path("/home/james/bet_project/football_data/scot_nostats/odds.csv")

df = pd.read_csv(MATCHES_FILE_PATH)
odds = pd.read_csv(ODDS_FILE_PATH)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])


def find_match_diff_odds(matches: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    """
    When the matches gets updated then the odds need to downloaded. Since it takes so long we need to only find
    the matches we havent done. So use odds from the last good place if any.
    """
    valid_ids = set(odds.sofa_match_id.unique())
    return matches[~matches["match_id"].isin(valid_ids)]


def download_odds_from_df(matches: pd.DataFrame, dir_name: str) -> None:
    """
    Here we call the processor class to save the reapir work
    """
    processor = DataFrameProcessor()
    processed_data, saved_files = processor.process_and_save(
        df=df,
        run_name=dir_name,
        save_formats=["csv"],
        home_column="home_team",
        away_column="away_team",
        cleanup_on_success=True,
    )


def merge_repair_odds_and_save(
    odds: pd.DataFrame, repair: pd.DataFrame, save_path: str
) -> None:
    """
    here we join the repair download and the main odds file
    """
    df_odds = pd.concat([odds, repair])

    print()
    print(f" size of df_odds : { len(df_odds['sofa_match_id'].unique()) } ")

    print(df_odds.head(10))

    df_odds.to_csv(save_path)


if __name__ == "__main__":
    missing_odds_match_ids: pd.DataFrame = find_match_diff_odds(df, odds)
    print(missing_odds_match_ids.shape)
    # TODO: run me on the other machine
