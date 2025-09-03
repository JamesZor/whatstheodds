import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, call, patch

import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf

from whatstheodds.betfair.dataclasses import (
    BetfairSearchRequest,
    BetfairSearchResult,
    BetfairSearchSingleMarketResult,
)
from whatstheodds.history.betfair_details import BetfairDetailsGrabber
from whatstheodds.mappers.match_mapper import MatchMapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#####
# CONFIG
TEST_DF_FILE_PATH = Path(
    "/home/james/bet_project/football_data/scot_nostats_20_to_24/football_data_mixed_matches.csv"
)


####
# Create the sofa dataframe
@pytest.fixture
def sofa_dataframe():
    df = pd.read_csv(TEST_DF_FILE_PATH)
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs"""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_search_result():
    """Create a mock BetfairSearchResult for testing"""
    result = BetfairSearchResult(
        sofa_match_id=10387533,
        home="St Johnstone",
        away="Celtic",
        date="2022-10-08 00:00:00",
        country="GB",
        match_id=31794437,
        valid_markets={
            "HALF_TIME": BetfairSearchSingleMarketResult(
                strategy_used="exact_date_team_search",
                market_type="HALF_TIME",
                file="/xds_nfs/edp_processed/BASIC/2022/Oct/8/31794437/1.204271996.bz2",
                match_id="31794437",
                market_id="204271996",
                error=None,
            ),
            "MATCH_ODDS": BetfairSearchSingleMarketResult(
                strategy_used="exact_date_team_search",
                market_type="MATCH_ODDS",
                file="/xds_nfs/edp_processed/BASIC/2022/Oct/8/31794437/1.204271989.bz2",
                match_id="31794437",
                market_id="204271989",
                error=None,
            ),
        },
        missing_markets={},
    )
    return result


class TestBetfairDetailsGrabber:

    def test_sofa_dataframe_load(self, sofa_dataframe):
        """Test loading the dataframe"""
        logger.info(f"Load the dataframe from:\n{str(TEST_DF_FILE_PATH)}.")
        logger.info(f"DataFrame shape: {sofa_dataframe.shape}")
        logger.info(f"Columns: {list(sofa_dataframe.columns)}")
        logger.info("\nFirst 5 rows:")
        logger.info(sofa_dataframe.head(5))

        # Basic checks
        assert not sofa_dataframe.empty, "DataFrame is empty"
        assert "match_id" in sofa_dataframe.columns, "match_id column missing"

    def test_init_and_print_conf(self, temp_dir):
        """Test initialization with custom directories"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing BetfairDetailsGrabber Initialization")
        logger.info("=" * 60)

        # Test with custom directories
        betfair_details_grabber = BetfairDetailsGrabber(
            output_dir=temp_dir / "output", input_dir=temp_dir / "input"
        )

        # Check attributes
        assert isinstance(
            betfair_details_grabber, BetfairDetailsGrabber
        ), "Failed to init betfair grabber"
        assert isinstance(
            betfair_details_grabber.cfg, DictConfig
        ), f"cfg is not type DictConfig"
        assert isinstance(
            betfair_details_grabber.match_mapper, MatchMapper
        ), "Failed to get match mapper"

        # Log directories
        logger.info(f"\nDirectory Configuration:")
        logger.info(f"  Output dir: {betfair_details_grabber.output_dir}")
        logger.info(f"  Input dir: {betfair_details_grabber.input_dir}")

        # Check directories exist
        assert (
            betfair_details_grabber.output_dir.exists()
        ), "Output directory not created"

        # Log configuration
        logger.info("\nConfiguration loaded:")
        logger.info(OmegaConf.to_yaml(betfair_details_grabber.cfg))

        # Log markets to be searched
        markets = betfair_details_grabber.cfg.betfair_football.markets
        logger.info(f"\nMarkets to search: {markets}")
        logger.info(f"Number of markets: {len(markets)}")

    def test_single_mapping_row(self, sofa_dataframe):
        """Test mapping a single row"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Single Row Mapping")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber()

        # Get a specific row for consistency
        row_to_map = sofa_dataframe.iloc[70]  # St Johnstone vs Celtic

        logger.info(f"\nRow to be mapped:")
        logger.info(f"  Match ID: {row_to_map['match_id']}")
        logger.info(f"  Teams: {row_to_map['home_team']} vs {row_to_map['away_team']}")
        logger.info(f"  Date: {row_to_map['match_date']}")
        logger.info(f"  Tournament: {row_to_map['tournament_slug']}")

        # Map the row
        map_results = betfair_details_grabber.map_match_from_row(row_to_map)

        logger.info(f"\nMapping result:")
        logger.info(f"  Type: {type(map_results).__name__}")

        assert isinstance(
            map_results, BetfairSearchRequest
        ), "Failed to get betfair search request"
        assert (
            map_results.sofa_match_id == row_to_map["match_id"]
        ), "Match ids do not match"

        logger.info("\nSearch Request Details:")
        logger.info(json.dumps(map_results.to_dict(), indent=2))

    def test_single_betfair_search(self, sofa_dataframe):
        """Test searching for a single match"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Single Betfair Search")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber()
        row_to_map = sofa_dataframe.iloc[70]  # St Johnstone vs Celtic

        # Map the row
        map_results = betfair_details_grabber.map_match_from_row(row_to_map)
        logger.info(
            f"\nSearching for: {map_results.home} vs {map_results.away} on {map_results.date}"
        )

        # Search
        search_result = betfair_details_grabber.search_match(map_results)

        if search_result:
            logger.info("\n✓ Search successful!")
            logger.info(f"  Betfair Match ID: {search_result.match_id}")
            logger.info(f"  Markets found: {len(search_result.valid_markets)}")
            logger.info(f"  Markets missing: {len(search_result.missing_markets)}")

            logger.info("\nMarkets found:")
            for market_type, market_data in search_result.valid_markets.items():
                logger.info(f"  - {market_type}: {market_data.file}")

            if search_result.missing_markets:
                logger.info("\nMarkets missing:")
                for market_type, market_data in search_result.missing_markets.items():
                    logger.info(f"  - {market_type}: {market_data.error}")
        else:
            logger.warning("✗ Search failed - no result returned")

    def test_process_from_row(self, sofa_dataframe, temp_dir):
        """Test the complete row processing pipeline"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Complete Row Processing")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Process multiple rows
        sample_rows = sofa_dataframe.head(3)
        logger.info(f"\nProcessing {len(sample_rows)} matches...")

        for idx, row in sample_rows.iterrows():
            logger.info(f"\nMatch {idx + 1}: {row['home_team']} vs {row['away_team']}")

            result = betfair_details_grabber.process_from_row(row)

            if result:
                logger.info(f"  ✓ Processed successfully")
                logger.info(f"  Betfair ID: {result.match_id}")
                logger.info(f"  Markets: {list(result.valid_markets.keys())}")
            else:
                logger.warning(f"  ✗ Processing failed")

    def test_identify_work_needed(self, sofa_dataframe, temp_dir):
        """Test work identification logic"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Work Identification")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Test with no existing results
        work = betfair_details_grabber.identify_work_needed(sofa_dataframe.head(10))

        logger.info("\nWork needed (no existing results):")
        logger.info(f"  New matches: {len(work['new'])}")
        logger.info(f"  Incomplete: {len(work['incomplete'])}")
        logger.info(f"  Failed: {len(work['failed'])}")
        logger.info(f"  Complete: {len(work['complete'])}")

        assert len(work["new"]) == 10, "Should have 10 new matches"
        assert len(work["incomplete"]) == 0, "Should have no incomplete"

        # Create mock existing results
        existing_results = {
            "metadata": {"markets_requested": ["MATCH_ODDS", "HALF_TIME"]},
            "matches": {
                str(sofa_dataframe.iloc[0]["match_id"]): {
                    "betfair_match_id": 12345,
                    "markets": {
                        "MATCH_ODDS": "/path/to/file",
                        "HALF_TIME": "/path/to/file",
                    },
                },
                str(sofa_dataframe.iloc[1]["match_id"]): {
                    "betfair_match_id": 12346,
                    "markets": {"MATCH_ODDS": "/path/to/file"},  # Missing HALF_TIME
                },
                str(sofa_dataframe.iloc[2]["match_id"]): {
                    "betfair_match_id": None,
                    "error": "Match not found",
                },
            },
        }

        work = betfair_details_grabber.identify_work_needed(
            sofa_dataframe.head(10), existing_results
        )

        logger.info("\nWork needed (with existing results):")
        logger.info(f"  New matches: {len(work['new'])}")
        logger.info(f"  Incomplete: {len(work['incomplete'])}")
        logger.info(f"  Failed: {len(work['failed'])}")
        logger.info(f"  Complete: {len(work['complete'])}")

        assert len(work["complete"]) == 1, "Should have 1 complete match"
        assert len(work["incomplete"]) == 1, "Should have 1 incomplete match"
        assert len(work["failed"]) == 1, "Should have 1 failed match"
        assert len(work["new"]) == 7, "Should have 7 new matches"

    @patch("whatstheodds.history.betfair_details.BetfairDetailsGrabber.search_match")
    def test_batch_processing_with_mock(
        self, mock_search, sofa_dataframe, temp_dir, sample_search_result
    ):
        """Test batch processing with mocked search"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Batch Processing (Mocked)")
        logger.info("=" * 60)

        # Mock the search to return sample result
        mock_search.return_value = sample_search_result

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Process small batch
        small_df = sofa_dataframe.head(5)

        results = betfair_details_grabber.process_batch(
            matches_df=small_df,
            output_filename="test_results.json",
            mode="new_only",
            checkpoint_interval=2,  # Save every 2 matches
        )

        logger.info(f"\nProcessing complete:")
        logger.info(f"  Total matches in results: {len(results['matches'])}")
        logger.info(f"  Last updated: {results['metadata']['last_updated']}")

        # Check output file exists
        output_file = temp_dir / "test_results.json"
        assert output_file.exists(), "Output file not created"

        # Load and verify
        with open(output_file) as f:
            saved_results = json.load(f)

        logger.info(f"\nSaved results structure:")
        logger.info(
            f"  Markets requested: {saved_results['metadata']['markets_requested']}"
        )
        logger.info(
            f"  Total processed: {saved_results['metadata']['total_processed']}"
        )

        # Check a match entry
        first_match_id = str(small_df.iloc[0]["match_id"])
        if first_match_id in saved_results["matches"]:
            match_data = saved_results["matches"][first_match_id]
            logger.info(f"\nSample match data (ID: {first_match_id}):")
            logger.info(f"  Betfair ID: {match_data.get('betfair_match_id')}")
            logger.info(
                f"  Teams: {match_data.get('home')} vs {match_data.get('away')}"
            )
            logger.info(f"  Markets: {list(match_data.get('markets', {}).keys())}")

    def test_processing_modes(self, sofa_dataframe, temp_dir):
        """Test different processing modes"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Processing Modes")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Create initial results with mixed statuses
        initial_results = {
            "metadata": {
                "markets_requested": ["MATCH_ODDS", "HALF_TIME"],
                "last_updated": None,
                "total_processed": 3,
            },
            "matches": {
                str(sofa_dataframe.iloc[0]["match_id"]): {
                    "betfair_match_id": 12345,
                    "markets": {
                        "MATCH_ODDS": "/path1",
                        "HALF_TIME": "/path2",
                    },  # Complete
                },
                str(sofa_dataframe.iloc[1]["match_id"]): {
                    "betfair_match_id": 12346,
                    "markets": {"MATCH_ODDS": "/path3"},  # Incomplete
                },
                str(sofa_dataframe.iloc[2]["match_id"]): {
                    "error": "Match not found"  # Failed
                },
            },
        }

        # Save initial results
        output_file = temp_dir / "mode_test.json"
        with open(output_file, "w") as f:
            json.dump(initial_results, f)

        # Test different modes
        modes = [
            "new_only",
            "new_and_incomplete",
            "new_and_failed",
            "all_except_complete",
        ]

        for mode in modes:
            logger.info(f"\nTesting mode: {mode}")

            # Mock the search to avoid actual API calls
            with patch.object(betfair_details_grabber, "search_match") as mock_search:
                mock_search.return_value = None

                # Just check what would be processed
                work = betfair_details_grabber.identify_work_needed(
                    sofa_dataframe.head(5),
                    betfair_details_grabber.load_results(output_file),
                )

                logger.info(f"  Work identified:")
                logger.info(f"    New: {len(work['new'])}")
                logger.info(f"    Incomplete: {len(work['incomplete'])}")
                logger.info(f"    Failed: {len(work['failed'])}")
                logger.info(f"    Complete: {len(work['complete'])}")

    def test_summary_methods(self, temp_dir):
        """Test summary and analysis methods"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Summary Methods")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Create sample results
        sample_results = {
            "metadata": {
                "markets_requested": ["MATCH_ODDS", "HALF_TIME"],
                "last_updated": "2024-01-15T10:30:00",
                "total_processed": 5,
            },
            "matches": {
                "10001": {
                    "betfair_match_id": 31794437,
                    "markets": {
                        "MATCH_ODDS": "/path1",
                        "HALF_TIME": "/path2",
                    },  # Complete
                },
                "10002": {
                    "betfair_match_id": 31794438,
                    "markets": {"MATCH_ODDS": "/path3"},  # Partial
                },
                "10003": {
                    "betfair_match_id": 31794439,
                    "markets": {
                        "MATCH_ODDS": "/path4",
                        "HALF_TIME": "/path5",
                    },  # Complete
                },
                "10004": {"error": "Match not found"},  # No data
                "10005": {"betfair_match_id": 31794440, "markets": {}},  # No markets
            },
        }

        # Get market coverage summary
        summary = betfair_details_grabber.get_market_coverage_summary(
            results=sample_results
        )

        logger.info("\nMarket Coverage Summary:")
        logger.info(f"  Total matches: {summary['match_summary']['total']}")
        logger.info(f"  Complete: {summary['match_summary']['complete']}")
        logger.info(f"  Partial: {summary['match_summary']['partial']}")
        logger.info(f"  No data: {summary['match_summary']['no_data']}")

        logger.info("\nPer-market coverage:")
        for market, stats in summary["market_coverage"].items():
            logger.info(f"  {market}:")
            logger.info(f"    Found: {stats['found']}/{stats['total']}")
            logger.info(f"    Percentage: {stats['percentage']:.1f}%")

        # Test incomplete matches
        output_file = temp_dir / "test_summary.json"
        with open(output_file, "w") as f:
            json.dump(sample_results, f)

        incomplete_df = betfair_details_grabber.get_incomplete_matches(output_file)

        logger.info(f"\nIncomplete matches: {len(incomplete_df)}")
        if not incomplete_df.empty:
            logger.info("\nIncomplete match details:")
            for _, row in incomplete_df.head().iterrows():
                logger.info(f"  ID {row['sofa_id']}: Missing {row['markets_missing']}")

        # Test downloadable matches
        downloadable_all = betfair_details_grabber.get_downloadable_matches(
            output_file, min_markets=None
        )
        downloadable_partial = betfair_details_grabber.get_downloadable_matches(
            output_file, min_markets=1
        )

        logger.info(f"\nDownloadable matches:")
        logger.info(f"  With all markets: {len(downloadable_all)}")
        logger.info(f"  With at least 1 market: {len(downloadable_partial)}")

    def test_export_reports(self, temp_dir):
        """Test export functionality"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Export Reports")
        logger.info("=" * 60)

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Create sample results with various statuses
        sample_results = {
            "metadata": {
                "markets_requested": ["MATCH_ODDS", "HALF_TIME"],
                "last_updated": "2024-01-15T10:30:00",
                "total_processed": 3,
            },
            "matches": {
                "10001": {
                    "betfair_match_id": 31794437,
                    "home": "Team A",
                    "away": "Team B",
                    "markets": {"MATCH_ODDS": "/path1"},  # Incomplete
                },
                "10002": {
                    "betfair_match_id": 31794438,
                    "home": "Team C",
                    "away": "Team D",
                    "markets": {
                        "MATCH_ODDS": "/path2",
                        "HALF_TIME": "/path3",
                    },  # Complete
                },
                "10003": {
                    "home": "Team E",
                    "away": "Team F",
                    "error": "Match not found",  # Failed
                },
            },
        }

        # Save results
        results_file = temp_dir / "export_test.json"
        with open(results_file, "w") as f:
            json.dump(sample_results, f)

        # Export reports
        betfair_details_grabber.export_analysis_reports(results_file)

        # Check exported files
        logger.info("\nChecking exported files:")

        # Check incomplete matches CSV
        incomplete_file = temp_dir / "incomplete_matches.csv"
        if incomplete_file.exists():
            incomplete_df = pd.read_csv(incomplete_file)
            logger.info(f"  ✓ incomplete_matches.csv: {len(incomplete_df)} rows")
            logger.info(f"    Columns: {list(incomplete_df.columns)}")
        else:
            logger.info("  ✗ incomplete_matches.csv not created")

        # Check summary JSON
        summary_file = temp_dir / "market_coverage_summary.json"
        if summary_file.exists():
            with open(summary_file) as f:
                summary = json.load(f)
            logger.info(f"  ✓ market_coverage_summary.json created")
            logger.info(f"    Complete matches: {summary['match_summary']['complete']}")
        else:
            logger.info("  ✗ market_coverage_summary.json not created")

        # Check ready for download JSON
        download_file = temp_dir / "ready_for_download.json"
        if download_file.exists():
            with open(download_file) as f:
                downloadable = json.load(f)
            logger.info(f"  ✓ ready_for_download.json: {len(downloadable)} matches")
        else:
            logger.info("  ✗ ready_for_download.json not created")

    def test_real_batch_small(self, sofa_dataframe, temp_dir):
        """Test with real API calls on small batch (be careful with rate limits)"""
        logger.info("\n" + "=" * 60)
        logger.info("Testing Real Batch Processing (Small)")
        logger.info("=" * 60)
        logger.info("⚠️  This test makes real API calls - using only 2 matches")

        betfair_details_grabber = BetfairDetailsGrabber(output_dir=temp_dir)

        # Use only 2 matches to avoid rate limits
        small_batch = sofa_dataframe.iloc[70:72]  # Specific rows for consistency

        logger.info(f"\nProcessing {len(small_batch)} matches:")
        for _, row in small_batch.iterrows():
            logger.info(
                f"  - {row['home_team']} vs {row['away_team']} ({row['match_date'].date()})"
            )

        # Process batch
        results = betfair_details_grabber.process_batch(
            matches_df=small_batch,
            output_filename="real_test_results.json",
            mode="new_only",
        )

        # The summary is printed automatically by process_batch

        # Verify results
        output_file = temp_dir / "real_test_results.json"
        assert output_file.exists(), "Output file not created"

        # Check results structure
        logger.info(f"\nResults verification:")
        logger.info(f"  Matches processed: {len(results['matches'])}")

        for sofa_id, match_data in results["matches"].items():
            logger.info(f"\n  Match {sofa_id}:")
            logger.info(
                f"    Teams: {match_data.get('home')} vs {match_data.get('away')}"
            )

            if "error" in match_data:
                logger.info(f"    Status: ✗ Error - {match_data['error']}")
            elif "markets" in match_data:
                markets = match_data["markets"]
                logger.info(f"    Status: ✓ Found")
                logger.info(f"    Betfair ID: {match_data.get('betfair_match_id')}")
                logger.info(f"    Markets: {len(markets)} found")
                for market_type in markets:
                    logger.info(f"      - {market_type}")

        # Export analysis
        logger.info("\nExporting analysis reports...")
        betfair_details_grabber.export_analysis_reports(output_file)

        logger.info("\nTest complete! Check output directory for files:")
        logger.info(f"  {temp_dir}")

        # List all created files
        created_files = list(temp_dir.glob("*"))
        logger.info("\nCreated files:")
        for file in created_files:
            logger.info(f"  - {file.name} ({file.stat().st_size} bytes)")
