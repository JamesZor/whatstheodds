#!/usr/bin/env python3
"""
Direct analysis - answers your main questions without any project imports
Just run this to get the team name analysis between df1 and df2
"""

import json
from collections import Counter
from pathlib import Path

import pandas as pd


def main_analysis():
    """Answer your main questions about df1 vs df2"""

    print("🎯 DIRECT ANALYSIS: df1 (failures) vs df2 (successes)")
    print("=" * 70)

    # Load your data
    match_file = "/home/james/bet_project/football/scot_test_mixed/football_data_mixed_matches.csv"
    join_file = "/home/james/bet_project/whatstheodds/output/scots_20250814_185431/join_table.csv"

    df = pd.read_csv(match_file)
    df["match_date"] = pd.to_datetime(df["match_date"])

    join_df = pd.read_csv(join_file)
    valid_ids = set(join_df.sofa_match_id.unique())

    # Your df1 and df2
    df1 = df[~df["match_id"].isin(valid_ids)]  # 304 failures
    df2 = df[df["match_id"].isin(valid_ids)]  # 843 successes

    print(f"df1 (failures): {len(df1)} matches")
    print(f"df2 (successes): {len(df2)} matches")
    print(f"Total: {len(df)} matches")

    # QUESTION 1: Are the team names in both df1 and df2?
    print(f"\n1. 🏆 TEAM NAME OVERLAP ANALYSIS")
    print("-" * 50)

    # Get all teams from each group
    df1_teams = set(df1["home_team_slug"].unique()) | set(
        df1["away_team_slug"].unique()
    )
    df2_teams = set(df2["home_team_slug"].unique()) | set(
        df2["away_team_slug"].unique()
    )

    print(f"Teams in df1 (failures): {len(df1_teams)}")
    print(f"Teams in df2 (successes): {len(df2_teams)}")

    # Teams that appear in BOTH groups
    common_teams = df1_teams & df2_teams
    print(f"Teams in BOTH df1 and df2: {len(common_teams)}")

    # Teams ONLY in failures
    failure_only = df1_teams - df2_teams
    print(f"Teams ONLY in df1 (failure-only): {len(failure_only)}")

    # Teams ONLY in successes
    success_only = df2_teams - df1_teams
    print(f"Teams ONLY in df2 (success-only): {len(success_only)}")

    print(f"\n📊 OVERLAP SUMMARY:")
    print(
        f"   Common teams: {len(common_teams)}/{len(df1_teams | df2_teams)} ({len(common_teams)/(len(df1_teams | df2_teams))*100:.1f}%)"
    )

    if len(failure_only) > 0:
        print(f"\n❌ FAILURE-ONLY TEAMS ({len(failure_only)}):")
        for team in sorted(failure_only)[:15]:  # Show first 15
            print(f"   - {team}")
        if len(failure_only) > 15:
            print(f"   ... and {len(failure_only) - 15} more")

    if len(success_only) > 0:
        print(f"\n✅ SUCCESS-ONLY TEAMS ({len(success_only)}):")
        for team in sorted(success_only)[:15]:  # Show first 15
            print(f"   - {team}")
        if len(success_only) > 15:
            print(f"   ... and {len(success_only) - 15} more")

    # QUESTION 2: Check if same matchups appear in both groups
    print(f"\n2. ⚽ MATCHUP ANALYSIS")
    print("-" * 50)

    def get_matchups(df):
        matchups = []
        for _, row in df.iterrows():
            # Normalize matchup by sorting team names
            teams = tuple(sorted([row["home_team_slug"], row["away_team_slug"]]))
            matchups.append(teams)
        return set(matchups)

    df1_matchups = get_matchups(df1)
    df2_matchups = get_matchups(df2)

    common_matchups = df1_matchups & df2_matchups

    print(f"Unique matchups in df1: {len(df1_matchups)}")
    print(f"Unique matchups in df2: {len(df2_matchups)}")
    print(f"Matchups appearing in BOTH: {len(common_matchups)}")

    if len(common_matchups) > 0:
        print(
            f"\n🤔 INSIGHT: {len(common_matchups)} team matchups appear in both success and failure groups!"
        )
        print(
            f"   This suggests the issue isn't just team names, but something else (dates, mapping, etc.)"
        )

        print(f"\n   Examples of matchups in both groups:")
        for matchup in list(common_matchups)[:5]:
            print(f"   - {matchup[0]} vs {matchup[1]}")

    # QUESTION 3: Load and check mapping file
    print(f"\n3. 🔄 MAPPING FILE ANALYSIS")
    print("-" * 50)

    mapping_data = load_mapping_directly()

    if mapping_data:
        mapped_teams = set(mapping_data.keys())

        df1_mapped = df1_teams & mapped_teams
        df1_unmapped = df1_teams - mapped_teams

        df2_mapped = df2_teams & mapped_teams
        df2_unmapped = df2_teams - mapped_teams

        print(f"Total teams in mapping file: {len(mapped_teams)}")
        print(f"")
        print(f"df1 (failures) mapping coverage:")
        print(
            f"   Mapped: {len(df1_mapped)}/{len(df1_teams)} ({len(df1_mapped)/len(df1_teams)*100:.1f}%)"
        )
        print(f"   Unmapped: {len(df1_unmapped)} teams")

        print(f"df2 (successes) mapping coverage:")
        print(
            f"   Mapped: {len(df2_mapped)}/{len(df2_teams)} ({len(df2_mapped)/len(df2_teams)*100:.1f}%)"
        )
        print(f"   Unmapped: {len(df2_unmapped)} teams")

        coverage_diff = (len(df1_mapped) / len(df1_teams)) - (
            len(df2_mapped) / len(df2_teams)
        )

        if coverage_diff < -0.1:  # df1 has 10%+ lower coverage
            print(
                f"\n🎯 KEY FINDING: df1 has {abs(coverage_diff)*100:.1f}% LOWER mapping coverage!"
            )
            print(f"   This is likely the main cause of search failures.")

        if len(df1_unmapped) > 0:
            print(f"\n❌ UNMAPPED TEAMS IN df1 (failures):")
            for team in sorted(df1_unmapped)[:20]:
                print(f"   - {team}")
            if len(df1_unmapped) > 20:
                print(f"   ... and {len(df1_unmapped) - 20} more")

    # QUESTION 4: Date patterns
    print(f"\n4. 📅 DATE PATTERN ANALYSIS")
    print("-" * 50)

    print(
        f"df1 (failures) date range: {df1['match_date'].min().date()} to {df1['match_date'].max().date()}"
    )
    print(
        f"df2 (successes) date range: {df2['match_date'].min().date()} to {df2['match_date'].max().date()}"
    )

    # Check if same dates have both successes and failures
    df1_dates = set(df1["match_date"].dt.date)
    df2_dates = set(df2["match_date"].dt.date)
    common_dates = df1_dates & df2_dates

    print(f"Dates with both successes and failures: {len(common_dates)}")

    if len(common_dates) > 0:
        print(f"   This confirms dates aren't the main issue")

    # SUMMARY AND RECOMMENDATIONS
    print(f"\n5. 📋 SUMMARY & RECOMMENDATIONS")
    print("-" * 50)

    print(f"FINDINGS:")
    print(f"1. Team overlap: {len(common_teams)} teams appear in both groups")
    print(f"2. Failure-only teams: {len(failure_only)} teams only appear in failures")
    print(f"3. Success-only teams: {len(success_only)} teams only appear in successes")
    print(f"4. Common matchups: {len(common_matchups)} matchups appear in both groups")

    if mapping_data:
        print(f"5. Mapping coverage difference: {abs(coverage_diff)*100:.1f}%")

    print(f"\nRECOMMENDATIONS:")
    if len(failure_only) > 0:
        print(
            f"1. HIGH PRIORITY: Add {len(failure_only)} failure-only teams to mapping file"
        )

    if len(common_matchups) > 0:
        print(
            f"2. MEDIUM PRIORITY: Investigate why same matchups have different outcomes"
        )
        print(f"   - Check date ranges in search")
        print(f"   - Add retry logic")
        print(f"   - Add rate limiting (time.sleep)")

    if mapping_data and coverage_diff < -0.1:
        print(f"3. IMMEDIATE: Fix mapping coverage - this is likely the main issue")

    return df1, df2, failure_only, common_matchups, mapping_data


def load_mapping_directly():
    """Load mapping file directly without imports"""

    # Try different possible locations
    possible_paths = [
        Path("/home/james/bet_project/whatstheodds/mappings"),
        Path("mappings"),
        Path("../mappings"),
        Path("../../mappings"),
        Path("/home/james/bet_project/whatstheodds/src/../mappings"),
    ]

    for mapping_dir in possible_paths:
        if mapping_dir.exists():
            json_files = list(mapping_dir.glob("*.json"))
            if json_files:
                try:
                    mapping_file = json_files[0]  # Use first JSON file
                    print(f"📁 Found mapping file: {mapping_file}")

                    with open(mapping_file, "r") as f:
                        return json.load(f)

                except Exception as e:
                    print(f"❌ Error loading {mapping_file}: {e}")
                    continue

    print(f"❌ No mapping file found in any of these locations:")
    for path in possible_paths:
        print(f"   - {path}")
    return {}


def generate_missing_mappings(failure_only_teams):
    """Generate mapping entries for missing teams"""

    if not failure_only_teams:
        print("No missing teams to map")
        return

    print(f"\n🔧 MAPPING GENERATOR")
    print("-" * 30)
    print(f"Add these {len(failure_only_teams)} teams to your mapping file:")
    print()
    print("{")

    for i, team in enumerate(sorted(failure_only_teams)):
        comma = "," if i < len(failure_only_teams) - 1 else ""
        # Generate placeholder mappings
        betfair_name = team.replace(" ", " ").title()  # Clean up the name
        print(f'  "{team}": "{betfair_name}"{comma}')

    print("}")
    print()
    print("📝 NOTE: Replace the right-hand side with actual Betfair team names")
    print("   Check Betfair website or existing successful mappings for correct names")


if __name__ == "__main__":
    # Run the analysis
    df1, df2, failure_only, common_matchups, mapping_data = main_analysis()

    # Generate mapping suggestions
    if failure_only:
        generate_missing_mappings(failure_only)

    print(f"\n✅ Analysis complete!")
    print(f"Key insight: Check if df1 has more unmapped teams than df2")
