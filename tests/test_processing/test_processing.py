from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.betfair.downloader import BetfairDownloader, BetfairDownloadError
from src.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from src.extractors import MatchExtractor
from src.mappers.match_mapper import MatchMapper
from src.processor.dataframe_processor import DataFrameProcessor

match_file_path = Path(
    "/home/james/bet_project/football/scot_test_mixed/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])

df.head(20)
d1 = df[0:5]


@pytest.mark.skip()
def test_one():
    row1 = df[
        (df["home_team_slug"] == "celtic") & (df["away_team_slug"] == "rangers")
    ].iloc[2]
    print(row1)
    mapper = MatchMapper()
    search_engine = BetfairSearchEngine()

    search_request = mapper.map_match_from_row(row1)

    print(search_request)

    search_results = search_engine.search_main(search_request)  # type: ignore[arg-type]

    print(search_results)

    downloadel = BetfairDownloader()
    downlaod_file = downloadel.download_odds(search_results)

    print(downloadel.verify_downloads(search_results))

    extractor = MatchExtractor()
    results = extractor.extract_match(search_results.match_id)

    results["match_info"]
    odds = results["odds_data"]
    hf: pd.DataFrame = odds[(odds["minutes"] < 47) & (odds["minutes"] > 10)]
    odds.plot.line(x="minutes", y="over_3_5")


@pytest.mark.skip()
def test_two():
    row1 = df[
        (df["home_team_slug"] == "celtic") & (df["away_team_slug"] == "rangers")
    ].iloc[2]
    print(row1)

    row_t = df.sample(1).iloc[0]
    row_t

    results = pipeline.process_from_row(row_t)
    results.extracted_data["match_info"]
    odds = results.extracted_data["odds_data"]

    odds.plot.line(x="minutes", y="over_1_5")


@pytest.mark.skip()
def test_three_():
    # Initialize processor
    processor = DataFrameProcessor()

    # Process DataFrame and save results
    processed_data, saved_files = processor.process_and_save(
        df=df,
        run_name="scots",
        save_formats=["csv"],
        cleanup_on_success=True,
    )


def test_four():
    """
    only aboyt 74% of matches had paired odds, believe this is due the search method, only allow +- 1 day.

    Here we can use the join.csv from the last run to see which matches failed.

    Without extended search = 304/304 [01:53<00:00,  2.69match/s, success=0, failed=304, current=1.25e+7]

    """
    join_file_path = "/home/james/bet_project/whatstheodds/output/scots_20250814_185431/join_table.csv"
    join_df = pd.read_csv(join_file_path)
    valid_ids = set(join_df.sofa_match_id.unique())
    df1 = df[~df["match_id"].isin(valid_ids)]
    # Initialize processor
    processor = DataFrameProcessor()
    # Process DataFrame and save results
    processed_data, saved_files = processor.process_and_save(
        df=df1,
        run_name="scots_1",
        save_formats=["csv"],
        cleanup_on_success=True,
    )
