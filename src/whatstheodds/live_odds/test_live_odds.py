import os

import betfairlightweight
from betfairlightweight import filters

from whatstheodds.betfair.api_client import setup_betfair_api_client
from whatstheodds.utils import load_config

# --- 1. Set up your credentials ---
# --- 2. Create the API client and Login ---
trading = setup_betfair_api_client()

# Login
# Use login_interactive() if you don't have SSL certificates
# See the quickstart guide for more details
# --- 3. Find the Event Type for Soccer ---
# We want to find the event type for Soccer to filter our search
soccer_event_type = trading.betting.list_event_types(
    filter=filters.market_filter(text_query="Soccer")
)
print(soccer_event_type)
# Check if we found the event type
if not soccer_event_type:
    print("Could not find event type for Soccer.")
else:
    soccer_event_type_id = soccer_event_type[0].event_type.id
    print(f"Found Soccer Event Type ID: {soccer_event_type_id}")

    # --- 4. Find the Event (the match) ---
    # Now, let's find a specific match. Replace "Team A" and "Team B" with the teams you are interested in.
    # For example, "Arsenal v Man City"
    team_name = "Liverpool v Everton"  # Replace with the team you are looking for

    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id], text_query=team_name
        )
    )

    if not events:
        print(f"Could not find any events for '{team_name}'")
    else:
        print(f"Found {len(events)} events for '{team_name}':")
        for event in events:
            print(f"- Event: {event.event.name}, ID: {event.event.id}")

            # --- 5. Get the Market Catalogue for the Match Odds ---
            # Now we find the "Match Odds" market for the event.
            market_catalogues = trading.betting.list_market_catalogue(
                filter=filters.market_filter(
                    event_ids=[event.event.id], market_type_codes=["MATCH_ODDS"]
                ),
                market_projection=["RUNNER_DESCRIPTION", "MARKET_START_TIME"],
                max_results=1,
            )

            if not market_catalogues:
                print(f"  - Could not find 'Match Odds' market for {event.event.name}")
            else:
                market = market_catalogues[0]
                print(f"  - Found Market: {market.market_name}, ID: {market.market_id}")

                # --- 6. Get the Market Book (the odds) ---
                market_books = trading.betting.list_market_book(
                    market_ids=[market.market_id],
                    price_projection=filters.price_projection(
                        price_data=filters.price_data(ex_all_offers=True)
                    ),
                )

                if not market_books:
                    print(f"    - Could not get market book for {market.market_name}")
                else:
                    market_book = market_books[0]
                    print("    - Odds:")
                    for runner in market_book.runners:
                        # Find the runner in the market catalogue to get the name
                        runner_name = ""
                        for r in market.runners:
                            if r.selection_id == runner.selection_id:
                                runner_name = r.runner_name
                                break
                        print(f"      - {runner_name}:")
                        # -- Back Odds --
                        if runner.ex.available_to_back:
                            # Get the best available back price (the first one in the list)
                            best_back = runner.ex.available_to_back[0]
                            print(
                                f"        - Best Back Price: {best_back.price}, Size: £{best_back.size:.2f}"
                            )
                        else:
                            print("        - No back odds available")

                        # -- Lay Odds --
                        if runner.ex.available_to_lay:
                            # Get the best available lay price (the first one in the list)
                            best_lay = runner.ex.available_to_lay[0]
                            print(
                                f"        - Best Lay Price: {best_lay.price}, Size: £{best_lay.size:.2f}"
                            )
                        else:
                            print("        - No lay odds available")


# --- 7. Logout ---
trading.logout()
print("Logged out.")
