import pandas as pd

# =============================================================================
# PHASE 1: CREATE MAPPING TABLE (Uses your existing search)
# =============================================================================


class MappingCreator:
    """
    Phase 1: Create mapping table from your database to Betfair IDs
    Uses your existing search engine - minimal changes needed
    """

    def __init__(self, betfair_search_engine):
        """
        Pass in your existing BetfairSearchEngine - no changes needed to it
        """
        self.search_engine = betfair_search_engine

    def create_mapping_table(
        self, matches_df: pd.DataFrame, output_path: Path, resume_existing: bool = True
    ) -> pd.DataFrame:
        """
        Create sofa_match_id -> betfair_match_id mapping table

        Args:
            matches_df: Your match DataFrame with columns:
                       - match_id (sofa ID)
                       - home_team_slug
                       - away_team_slug
                       - match_date
                       - tournament_id (optional)
            output_path: Where to save mapping CSV
            resume_existing: Resume from existing file if it exists

        Returns:
            DataFrame with mapping results
        """

        # Resume from existing file
        if resume_existing and output_path.exists():
            existing = pd.read_csv(output_path)
            processed_ids = set(existing["sofa_match_id"])
            logger.info(f"Resuming: {len(processed_ids)} matches already processed")
        else:
            existing = pd.DataFrame()
            processed_ids = set()

        # Filter to remaining matches
        remaining = matches_df[~matches_df["match_id"].isin(processed_ids)].copy()
        logger.info(f"Processing {len(remaining)} remaining matches")

        if len(remaining) == 0:
            logger.info("All matches already processed")
            return existing

        mappings = []

        with tqdm(total=len(remaining), desc="Creating mappings") as pbar:

            for _, match in remaining.iterrows():
                try:
                    # Create search request using your existing structure
                    # Adapt this to match your BetfairSearchRequest exactly
                    search_request = self._create_search_request(match)

                    # Use your existing search engine - no changes needed
                    search_result = self.search_engine.search_main(search_request)

                    # Extract mapping data
                    if search_result.match_id:  # Found a match
                        mapping = {
                            "sofa_match_id": match["match_id"],
                            "betfair_match_id": search_result.match_id,
                            "confidence": search_result.confidence,
                            "home_team": match["home_team_slug"],
                            "away_team": match["away_team_slug"],
                            "match_date": match["match_date"],
                            "available_markets": (
                                ",".join(search_result.valid_markets.keys())
                                if search_result.valid_markets
                                else ""
                            ),
                            "search_strategy": search_result.strategy_used,
                            "status": "found",
                            "created_at": datetime.now().isoformat(),
                        }
                        pbar.set_postfix(status="Found")
                    else:  # No match found
                        mapping = {
                            "sofa_match_id": match["match_id"],
                            "betfair_match_id": None,
                            "confidence": 0.0,
                            "home_team": match["home_team_slug"],
                            "away_team": match["away_team_slug"],
                            "match_date": match["match_date"],
                            "available_markets": "",
                            "search_strategy": "not_found",
                            "status": "not_found",
                            "created_at": datetime.now().isoformat(),
                        }
                        pbar.set_postfix(status="Not found")

                    mappings.append(mapping)

                    # Save progress every 25 matches
                    if len(mappings) % 25 == 0:
                        self._save_progress(existing, mappings, output_path)

                except Exception as e:
                    logger.error(f"Error processing match {match['match_id']}: {e}")
                    # Add error mapping
                    mappings.append(
                        {
                            "sofa_match_id": match["match_id"],
                            "betfair_match_id": None,
                            "confidence": 0.0,
                            "home_team": match["home_team_slug"],
                            "away_team": match["away_team_slug"],
                            "match_date": match["match_date"],
                            "available_markets": "",
                            "search_strategy": f"error: {str(e)[:100]}",
                            "status": "error",
                            "created_at": datetime.now().isoformat(),
                        }
                    )
                    pbar.set_postfix(status="Error")

                pbar.update(1)

        # Combine and save final results
        final_mapping = self._combine_mappings(existing, mappings)
        final_mapping.to_csv(output_path, index=False)

        # Log statistics
        found = len(final_mapping[final_mapping["betfair_match_id"].notna()])
        total = len(final_mapping)
        logger.info(
            f"Mapping complete: {found}/{total} matches found ({found/total*100:.1f}%)"
        )

        return final_mapping

    def _create_search_request(self, match_row):
        """
        Create search request - ADAPT THIS to match your BetfairSearchRequest structure
        """
        # Import your existing dataclasses
        from whatstheodds.betfair.dataclasses import (  # Update this import
            BetfairSearchRequest,
        )

        return BetfairSearchRequest(
            home_team=match_row["home_team_slug"],
            away_team=match_row["away_team_slug"],
            match_date=pd.to_datetime(match_row["match_date"]),
            tournament_id=match_row.get("tournament_id"),
            # Add other fields your BetfairSearchRequest needs
        )

    def _save_progress(self, existing_df, new_mappings, output_path):
        """Save progress periodically"""
        combined = self._combine_mappings(existing_df, new_mappings)
        combined.to_csv(output_path, index=False)
        logger.info(f"Progress saved: {len(combined)} total mappings")

    def _combine_mappings(self, existing_df, new_mappings):
        """Combine existing and new mappings"""
        if new_mappings:
            new_df = pd.DataFrame(new_mappings)
            if not existing_df.empty:
                return pd.concat([existing_df, new_df], ignore_index=True)
            else:
                return new_df
        return existing_df
