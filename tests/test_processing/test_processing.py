from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from whatstheodds.betfair.downloader import BetfairDownloader, BetfairDownloadError
from whatstheodds.betfair.search_engine import BetfairSearchEngine, BetfairSearchRequest
from whatstheodds.extractors import MatchExtractor
from whatstheodds.mappers.match_mapper import MatchMapper
from whatstheodds.processor.dataframe_processor import DataFrameProcessor

match_file_path = Path(
    "/home/james/bet_project/football_data/scot_nostats/football_data_mixed_matches.csv"
)
df = pd.read_csv(match_file_path)
# Convert date column once
df["match_date"] = pd.to_datetime(df["match_date"])


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
        run_name="scots_nostats",
        save_formats=["csv"],
        cleanup_on_success=True,
    )


@pytest.mark.skip()
def test_four():
    """
    only aboyt 74% of matches had paired odds, believe this is due the search method, only allow +- 1 day.

    Here we can use the join.csv from the last run to see which matches failed.

    Without extended search = 304/304 [01:53<00:00,  2.69match/s, success=0, failed=304, current=1.25e+7]

    """
    join_file_path = "/home/james/bet_project/whatstheodds/output/scots_nostats_20250816_200817/join_table.csv"
    join_df = pd.read_csv(join_file_path)
    valid_ids = set(join_df.sofa_match_id.unique())
    df1 = df[~df["match_id"].isin(valid_ids)]
    # Initialize processor
    #    print(len(df1))
    #    print(df1)
    processor = DataFrameProcessor()
    # Process DataFrame and save results
    processed_data, saved_files = processor.process_and_save(
        df=df1,
        run_name="scots_1",
        save_formats=["csv"],
        cleanup_on_success=True,
    )


# @pytest.mark.skip()
def test_five():
    """
    Some new team names appear in the matches sofa data set.
    here we collect them and display them to be added to the mapping
    """
    join_file_path = "/home/james/bet_project/whatstheodds/output/scots_nostats_20250816_200817/join_table.csv"
    join_df = pd.read_csv(join_file_path)
    valid_ids = set(join_df.sofa_match_id.unique())
    df1 = df[~df["match_id"].isin(valid_ids)]

    #    teams_set = set( df1["home_team_slug"].unique())
    #    team2 = set(df1["away_team_slug"].unique())
    #    teams = teams_set | team2
    #    for team in teams:
    #        print(team)
    mm = MatchMapper()

    none_counter = 0
    for idx, row in df1.iterrows():
        print(f"{row['home_team_slug']} vs {row['away_team_slug']}")
        r = mm.map_match_from_row(row)
        print(f"Result: {r}")

        none_counter += 1 if r is None else 0

    print(f"None count = {none_counter}")


@pytest.mark.skip()
def test_six():
    """
    here we eed to join the main run and the repair run
    """
    main_path = "/home/james/bet_project/whatstheodds/output/scots_nostats_20250816_200817/odds_table.csv"
    repair_path = "/home/james/bet_project/whatstheodds/output/scots_2_20250817_110515/odds_table.csv"

    main_df = pd.read_csv(main_path)
    repair_df = pd.read_csv(repair_path)

    df_odds = pd.concat([main_df, repair_df])

    print()
    print(f" size of df_odds : { len(df_odds['sofa_match_id'].unique()) } ")
    print(f" size of df : { df.shape } ")

    print(df_odds.head(10))

    df_odds.to_csv("/home/james/bet_project/football_data/scot_nostats/odds.csv")


def test_df_mapping_and_search():
    mapper = MatchMapper()
    search_engine = BetfairSearchEngine()
    search_request_fails = []
    search_results_fails = []

    for idx, row in df.head(5).iterrows():
        search_request = mapper.map_match_from_row(row)

        mapper.map_match_from_row(
            row2,
            date_column=date_column,
            match_id_column=match_id_column,
            home_column=home_column,
            away_column=away_column,
            tournament_column=tournament_column,
        )
        if search_request is None:
            search_request_fails.append(row["match_id"])
            continue

        search_results = search_engine.search_main(search_request)

        if search_results is None:
            search_results_fails.append(row["match_id"])
            continue
    row2 = df[df["match_id"] == 10387456].iloc[0]
    row2

    date_column: str = "match_date"
    match_id_column: str = "match_id"
    home_column: str = "home_team_slug"
    away_column: str = "away_team_slug"
    tournament_column: str = "tournament_id"

    row2.columns

    mapper.map_match_from_row(
        row2,
        date_column=date_column,
        match_id_column=match_id_column,
        home_column=home_column,
        away_column=away_column,
        tournament_column=tournament_column,
    )
    print(search_request_fails)
    print(search_results_fails)
