import os

import betfairlightweight
from betfairlightweight import filters

# --- Helper function for cleaner setup (assuming you have this) ---
from whatstheodds.betfair.api_client import setup_betfair_api_client

# --- 1. Set up your credentials & API client ---
trading = setup_betfair_api_client()

# --- 2. Define the markets you want ---
# A list of all market types to search for
DESIRED_MARKETS = [
    "MATCH_ODDS",
    "OVER_UNDER_05",
    "OVER_UNDER_15",
    "OVER_UNDER_25",
    "OVER_UNDER_35",
    "CORRECT_SCORE",
    "BOTH_TEAMS_TO_SCORE",
]

# --- 3. Find the Event Type for Soccer ---
soccer_event_type = trading.betting.list_event_types(
    filter=filters.market_filter(text_query="Soccer")
)

if not soccer_event_type:
    print("Could not find event type for Soccer.")
else:
    soccer_event_type_id = soccer_event_type[0].event_type.id
    print(f"Found Soccer Event Type ID: {soccer_event_type_id}")

    # --- 4. Find the Event (the match) ---
    team_name = "Dundee v Livingston"  # Replace with the team you are looking for

    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id], text_query=team_name
        )
    )

    if not events:
        print(f"Could not find any events for '{team_name}'")
    else:
        # Assuming we only want the first event found
        event = events[0]
        print(f"Found Event: {event.event.name}, ID: {event.event.id}\n")

        # --- 5. Get Market Catalogues for ALL desired markets ---
        market_catalogues = trading.betting.list_market_catalogue(
            filter=filters.market_filter(
                event_ids=[event.event.id],
                market_type_codes=DESIRED_MARKETS,  # Use the list of markets here
            ),
            market_projection=["RUNNER_DESCRIPTION"],
            max_results=100,  # Increase max_results to get all markets
        )

        if not market_catalogues:
            print(f"Could not find any of the desired markets for {event.event.name}")
        else:
            # Create a list of all market IDs
            market_ids = [market.market_id for market in market_catalogues]

            # --- 6. Get Market Books (the odds) for ALL markets in one call ---
            market_books = trading.betting.list_market_book(
                market_ids=market_ids,
                price_projection=filters.price_projection(
                    price_data=filters.price_data(ex_all_offers=True)
                ),
            )

            # Create a mapping from market_id to market_catalogue for easy lookup
            catalogue_map = {mc.market_id: mc for mc in market_catalogues}

            # --- 7. Loop through and display each market book ---
            for market_book in market_books:
                market_catalogue = catalogue_map.get(market_book.market_id)
                print(f"--- Market: {market_catalogue.market_name} ---")

                for runner in market_book.runners:
                    # Find the runner's name from the catalogue
                    runner_name = ""
                    for r in market_catalogue.runners:
                        if r.selection_id == runner.selection_id:
                            runner_name = r.runner_name
                            break

                    print(f"  - {runner_name}:")
                    # -- Back Odds --
                    if runner.ex.available_to_back:
                        best_back = runner.ex.available_to_back[0]
                        print(
                            f"    - Back: Price={best_back.price}, Size=£{best_back.size:.2f}"
                        )
                    else:
                        print("    - Back: No odds available")

                    # -- Lay Odds --
                    if runner.ex.available_to_lay:
                        best_lay = runner.ex.available_to_lay[0]
                        print(
                            f"    - Lay:  Price={best_lay.price}, Size=£{best_lay.size:.2f}"
                        )
                    else:
                        print("    - Lay:  No odds available")
                print("-" * 30 + "\n")  # Separator for clarity

# --- 8. Logout ---
trading.logout()
print("Logged out.")
