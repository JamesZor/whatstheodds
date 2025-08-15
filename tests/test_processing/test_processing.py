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
