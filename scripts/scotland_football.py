import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from whatstheodds.betfair.downloader import BetfairDownloader, BetfairDownloadError
from whatstheodds.betfair.rate_limiter import betfair_rate_limiter
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.extractors import MatchExtractor
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor import DataFrameProcessor

# Configure root logger to show everything
# logging.basicConfig(
#     level=logging.ERROR,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.StreamHandler(),  # This sends to console
#     ],
# )
#
# # Set specific loggers
# logging.getLogger("whatstheodds.betfair.downloader").setLevel(logging.DEBUG)
# logging.getLogger("whatstheodds.betfair.rate_limiter").setLevel(logging.DEBUG)

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
    usage = betfair_rate_limiter.get_current_usage()
    logging.info(
        f"Starting downloads - Rate limiter usage: {usage['usage_percent']:.1f}%"
    )
    processed_data, saved_files = processor.process_and_save(
        df=matches,
        run_name=dir_name,
        save_formats=["csv"],
        home_column="home_team",
        away_column="away_team",
        cleanup_on_success=True,
    )

    final_usage = betfair_rate_limiter.get_current_usage()
    logging.info(
        f"Downloads complete - Final rate limiter usage: {final_usage['usage_percent']:.1f}%"
    )

    print(saved_files)
    print("Done")


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


def join_ft_and_ht_odds():
    odds_path_1 = Path(
        "/home/james/bet_project/whatstheodds/output/scot_full_20250819_235516/odds_table.csv"
    )
    odds_path_2 = Path(
        "/home/james/bet_project/whatstheodds/output/scot_half_20250822_172420/odds_table.csv"
    )

    odd1 = pd.read_csv(odds_path_1)
    odd2 = pd.read_csv(odds_path_2)

    print(odd1.head(5))

    print(odd1.columns)
    print("-" * 40)
    print(odd2.head(5))

    print(odd2.columns)
    odd = pd.merge(
        left=odd1,
        right=odd2,
        how="inner",
        on=["sofa_match_id", "betfair_match_id"],
        suffixes=("", "_dup"),  # Keep left columns as-is, mark right duplicates
    )

    # Drop the duplicate columns
    columns_to_drop = [col for col in odd.columns if col.endswith("_dup")]
    odd = odd.drop(columns=columns_to_drop)
    print("-" * 40)
    print(odd.head(5))

    odd.to_csv("/home/james/bet_project/football_data/scot_nostats_20_to_24/odds.csv")


if __name__ == "__main__":
    missing_odds_match_ids: pd.DataFrame = find_match_diff_odds(df, odds)
    print(missing_odds_match_ids.shape)

    # Rate limit on downloads, thus need to rerun
    # download_odds_from_df(df, "scot_half")
    """
    re running to do first half

    """
    join_ft_and_ht_odds()

    # TODO: run me on the other machine
