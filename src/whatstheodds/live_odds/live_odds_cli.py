import argparse
import json
from datetime import datetime, timedelta

import betfairlightweight
from betfairlightweight import filters

from whatstheodds.betfair.api_client import setup_betfair_api_client

# --- 1. Define all the markets you want ---
DESIRED_MARKETS = [
    # Half-time markets
    # "HALF_TIME",
    # "FIRST_HALF_GOALS_05",
    # "FIRST_HALF_GOALS_15",
    # "FIRST_HALF_GOALS_25",
    # "HALF_TIME_SCORE",
    # Full-time markets
    "MATCH_ODDS",
    "OVER_UNDER_05",
    "OVER_UNDER_15",
    "OVER_UNDER_25",
    "OVER_UNDER_35",
    "CORRECT_SCORE",
    "BOTH_TEAMS_TO_SCORE",
]


def list_todays_games(trading, soccer_event_type_id):
    """Fetches and prints all soccer games scheduled for today."""
    print("Finding today's soccer matches...")

    # Define time range for today (UTC)
    now = datetime.utcnow()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)

    time_filter = filters.time_range(from_=start_of_day, to=end_of_day)

    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id],
            market_start_time=time_filter,
        )
    )

    if not events:
        print("No soccer matches found for today.")
        return

    print(f"Found {len(events)} matches today:")
    for event_result in sorted(events, key=lambda e: e.event.open_date):
        event = event_result.event
        print(f"  - {event.open_date.strftime('%H:%M')} | {event.name}")


def get_market_odds(trading, soccer_event_type_id, event_name, as_dict):
    """Fetches and displays odds for a specific event."""
    print(f"Searching for event: '{event_name}'")
    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id], text_query=event_name
        )
    )

    if not events:
        print(f"Could not find any events for '{event_name}'")
        return

    event = events[0].event
    print(f"Found Event: {event.name}, ID: {event.id}")

    market_catalogues = trading.betting.list_market_catalogue(
        filter=filters.market_filter(
            event_ids=[event.id], market_type_codes=DESIRED_MARKETS
        ),
        market_projection=["RUNNER_DESCRIPTION"],
        max_results=100,
    )

    if not market_catalogues:
        print(f"Could not find any desired markets for {event.name}")
        return

    market_ids = [market.market_id for market in market_catalogues]
    market_books = trading.betting.list_market_book(
        market_ids=market_ids,
        price_projection=filters.price_projection(
            price_data=filters.price_data(ex_all_offers=True)
        ),
    )

    catalogue_map = {mc.market_id: mc for mc in market_catalogues}
    results = {"event_name": event.name, "event_id": event.id, "markets": []}

    for market_book in market_books:
        market_catalogue = catalogue_map.get(market_book.market_id)
        market_data = {
            "market_name": market_catalogue.market_name,
            "market_id": market_book.market_id,
            "runners": [],
        }

        for runner in market_book.runners:
            runner_name = next(
                (
                    r.runner_name
                    for r in market_catalogue.runners
                    if r.selection_id == runner.selection_id
                ),
                "Unknown Runner",
            )
            runner_data = {"runner_name": runner_name, "back": None, "lay": None}

            if runner.ex.available_to_back:
                best_back = runner.ex.available_to_back[0]
                runner_data["back"] = {
                    "price": best_back.price,
                    "size": round(best_back.size, 2),
                }

            if runner.ex.available_to_lay:
                best_lay = runner.ex.available_to_lay[0]
                runner_data["lay"] = {
                    "price": best_lay.price,
                    "size": round(best_lay.size, 2),
                }

            market_data["runners"].append(runner_data)
        results["markets"].append(market_data)

    if as_dict:
        print(json.dumps(results, indent=2))
    else:
        display_results(results)


def display_results(results):
    """Prints results in a human-readable format."""
    print("\n" + "=" * 40)
    print(f"Event: {results['event_name']}")
    print("=" * 40 + "\n")

    for market in results["markets"]:
        print(f"--- Market: {market['market_name']} ---")
        for runner in market["runners"]:
            print(f"  - {runner['runner_name']}:")
            back_info = runner.get("back")
            lay_info = runner.get("lay")

            if back_info:
                print(
                    f"    - Back: Price={back_info['price']}, Size=£{back_info['size']:.2f}"
                )
            else:
                print("    - Back: No odds available")

            if lay_info:
                print(
                    f"    - Lay:  Price={lay_info['price']}, Size=£{lay_info['size']:.2f}"
                )
            else:
                print("    - Lay:  No odds available")
        print("-" * 30 + "\n")


def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(description="Betfair Odds CLI Tool for Soccer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sub-parser for the "list" command
    parser_list = subparsers.add_parser(
        "list", help="List all soccer matches for today."
    )

    # Sub-parser for the "odds" command
    parser_odds = subparsers.add_parser("odds", help="Get odds for a specific match.")
    parser_odds.add_argument(
        "event_name",
        nargs="+",
        help="The name of the event (e.g., 'Liverpool v Everton').",
    )
    parser_odds.add_argument(
        "-d",
        "--dict",
        action="store_true",
        help="Output the results as a dictionary (JSON).",
    )

    args = parser.parse_args()

    # --- Setup API client and Login ---
    trading = setup_betfair_api_client()
    try:
        # trading.login()
        # --- Find Soccer Event Type ID ---
        soccer_event_type = trading.betting.list_event_types(
            filter=filters.market_filter(text_query="Soccer")
        )[0]
        soccer_event_type_id = soccer_event_type.event_type.id

        # --- Execute Command ---
        if args.command == "list":
            list_todays_games(trading, soccer_event_type_id)
        elif args.command == "odds":
            event_name = " ".join(args.event_name)
            get_market_odds(trading, soccer_event_type_id, event_name, args.dict)

    finally:
        trading.logout()
        print("Logged out.")


if __name__ == "__main__":
    main()
