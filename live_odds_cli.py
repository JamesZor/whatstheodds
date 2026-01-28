import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Try to import rich, fail gracefully if not installed
try:
    from rich.console import Console
    from rich.table import Table

    RICH_INSTALLED = True
except ImportError:
    RICH_INSTALLED = False

import betfairlightweight
from betfairlightweight import filters

from whatstheodds.betfair.api_client import setup_betfair_api_client

# --- Define all the markets you want ---
DESIRED_MARKETS = [
    # Half-time markets
    "HALF_TIME",
    "FIRST_HALF_GOALS_05",
    "FIRST_HALF_GOALS_15",
    "FIRST_HALF_GOALS_25",
    "HALF_TIME_SCORE",
    # Full-time markets
    "MATCH_ODDS",
    "OVER_UNDER_05",
    "OVER_UNDER_15",
    "OVER_UNDER_25",
    "OVER_UNDER_35",
    "CORRECT_SCORE",
    "BOTH_TEAMS_TO_SCORE",
]


def load_filter_teams(filter_files):
    """Loads team names from specified JSON files in the 'mapping' directory."""
    if not filter_files:
        return None

    allowed_teams = set()
    base_path = Path("/home/james/bet_project/whatstheodds/mappings/")
    for file_name in filter_files:
        file_path = base_path / f"{file_name}.json"
        if file_path.exists():
            with open(file_path, "r") as f:
                data = json.load(f)
                # We use the values from the mapping, as requested
                allowed_teams.update(data.values())
            print(f"Loaded {len(data)} teams from '{file_path}'", file=sys.stderr)
        else:
            print(f"Warning: Filter file not found at '{file_path}'", file=sys.stderr)
    return allowed_teams


def list_todays_games(trading, soccer_event_type_id, filter_teams):
    """Fetches and prints soccer games for today, with optional team filtering."""
    print("\nFinding today's soccer matches...")
    now = datetime.utcnow()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)
    time_filter = filters.time_range(from_=start_of_day, to=end_of_day)

    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id], market_start_time=time_filter
        )
    )

    if not events:
        print("No soccer matches found for today.")
        return

    filtered_events = []
    if filter_teams:
        for event_result in events:
            # Check if either team in "Team A v Team B" is in our filter list
            teams = [team.strip() for team in event_result.event.name.split(" v ")]
            if any(team in filter_teams for team in teams):
                filtered_events.append(event_result)
    else:
        filtered_events = events

    if not filtered_events:
        print("No matches found for today with the specified filter.")
        return

    print(f"Found {len(filtered_events)} matches:")
    for event_result in sorted(filtered_events, key=lambda e: e.event.open_date):
        event = event_result.event
        print(f"  - {event.open_date.strftime('%H:%M')} | {event.name}")


def get_market_odds(trading, soccer_event_type_id, event_name, output_format):
    """Fetches odds and directs them to the correct display/output function."""
    print(f"Searching for event: '{event_name}'", file=sys.stderr)
    events = trading.betting.list_events(
        filter=filters.market_filter(
            event_type_ids=[soccer_event_type_id], text_query=event_name
        )
    )
    if not events:
        print(f"Could not find any events for '{event_name}'", file=sys.stderr)
        return

    event = events[0].event
    print(f"Found Event: {event.name}, ID: {event.id}", file=sys.stderr)

    market_catalogues = trading.betting.list_market_catalogue(
        filter=filters.market_filter(
            event_ids=[event.id], market_type_codes=DESIRED_MARKETS
        ),
        market_projection=[
            "RUNNER_DESCRIPTION",
            "MARKET_START_TIME",
            "MARKET_DESCRIPTION",
        ],
        max_results=100,
    )

    if not market_catalogues:
        print(f"Could not find any desired markets for {event.name}")
        return

    market_books = []
    print(
        f"Found {len(market_catalogues)} markets. Fetching odds individually...",
        file=sys.stderr,
    )
    for catalogue in market_catalogues:
        # Fetching odds for each market ID individually is the safest way to avoid TOO_MUCH_DATA
        book = trading.betting.list_market_book(
            market_ids=[catalogue.market_id],
            price_projection=filters.price_projection(
                price_data=filters.price_data(ex_all_offers=True)
            ),
        )
        if book:
            market_books.extend(book)

    # Build an intermediate, universal data structure first
    results = {"event_name": event.name, "event_id": event.id, "markets": []}
    catalogue_map = {mc.market_id: mc for mc in market_catalogues}

    for market_book in market_books:
        market_catalogue = catalogue_map.get(market_book.market_id)
        market_data = {
            "market_name": market_catalogue.market_name,
            "market_id": market_book.market_id,
            "market_type": market_catalogue.description.market_type,
            "runners": [],
        }
        for runner in market_book.runners:
            runner_name = next(
                (
                    r.runner_name
                    for r in market_catalogue.runners
                    if r.selection_id == runner.selection_id
                ),
                "Unknown",
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

    # Direct to the correct output based on the format argument
    if output_format == "dict":
        nested_dict = transform_to_nested_dict(results)
        print(json.dumps(nested_dict, indent=2))
    elif output_format == "pretty":
        if RICH_INSTALLED:
            display_pretty_results(results)
        else:
            print("\n'rich' library not installed. Falling back to simple display.")
            print("Install it with: pip install rich")
            display_simple_results(results)
    else:  # "simple" or default
        display_simple_results(results)


def display_simple_results(results):
    """Prints results in a simple, human-readable format."""
    print(f"\n{'='*40}\nEvent: {results['event_name']}\n{'='*40}\n")

    for market in results["markets"]:
        print(f"--- Market: {market['market_name']} ---")

        for runner in market["runners"]:
            print(f"  - {runner['runner_name']}:")

            back_info = runner.get("back")
            lay_info = runner.get("lay")

            # SAFETY CHECK: Only try to print details if back_info is not None
            if back_info:
                print(
                    f"    - Back: Price={back_info['price']}, Size=£{back_info['size']:.2f}"
                )
            else:
                print("    - Back: No odds available")

            # SAFETY CHECK: Only try to print details if lay_info is not None
            if lay_info:
                print(
                    f"    - Lay:  Price={lay_info['price']}, Size=£{lay_info['size']:.2f}"
                )
            else:
                print("    - Lay:  No odds available")

        print("-" * 30 + "\n")


def display_pretty_results(results):
    """Prints results in a rich table format."""
    console = Console()
    console.rule(f"[bold cyan]Event: {results['event_name']}", style="cyan")
    for market in results["markets"]:
        table = Table(
            title=f"Market: {market['market_name']}",
            title_style="magenta",
            show_header=True,
            header_style="bold blue",
        )
        table.add_column("Runner", style="white", min_width=20)
        table.add_column("Back Price", justify="right", style="cyan")
        table.add_column("Back Size (£)", justify="right", style="cyan")
        table.add_column("Lay Price", justify="right", style="yellow")
        table.add_column("Lay Size (£)", justify="right", style="yellow")

        for runner in market["runners"]:
            back_price = f"{runner['back']['price']:.2f}" if runner["back"] else "N/A"
            back_size = f"{runner['back']['size']:.2f}" if runner["back"] else "N/A"
            lay_price = f"{runner['lay']['price']:.2f}" if runner["lay"] else "N/A"
            lay_size = f"{runner['lay']['size']:.2f}" if runner["lay"] else "N/A"
            table.add_row(
                runner["runner_name"], back_price, back_size, lay_price, lay_size
            )

        console.print(table)


def transform_to_nested_dict(results):
    """Transforms the flat result structure into the requested nested dictionary."""
    output = {}
    for market in results["markets"]:
        market_type_str = market["market_type"]
        period = "ht" if "HALF" in market_type_str else "ft"

        if period not in output:
            output[period] = {}

        market_name = market["market_name"]
        if market_name not in output[period]:
            output[period][market_name] = {}

        for runner in market["runners"]:
            runner_name = runner["runner_name"]
            output[period][market_name][runner_name] = {
                "back": runner.get("back"),
                "lay": runner.get("lay"),
            }
    return output


def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(description="Betfair Odds CLI Tool for Soccer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sub-parser for the "list" command
    parser_list = subparsers.add_parser("list", help="List soccer matches for today.")
    parser_list.add_argument(
        "-f",
        "--filter",
        nargs="+",
        help="Filter by country using mapping files (e.g., 'scotland' 'england').",
    )

    # Sub-parser for the "odds" command
    parser_odds = subparsers.add_parser("odds", help="Get odds for a specific match.")
    parser_odds.add_argument(
        "event_name",
        nargs="+",
        help="The name of the event (e.g., 'Liverpool v Everton').",
    )
    output_group = parser_odds.add_mutually_exclusive_group()
    output_group.add_argument(
        "-d",
        "--dict",
        action="store_const",
        dest="output_format",
        const="dict",
        help="Output as a nested dictionary (JSON).",
    )
    output_group.add_argument(
        "-p",
        "--pretty",
        action="store_const",
        dest="output_format",
        const="pretty",
        help="Display results in a rich table.",
    )
    parser_odds.set_defaults(output_format="simple")

    args = parser.parse_args()

    trading = setup_betfair_api_client()
    try:
        soccer_event_type_id = trading.betting.list_event_types(
            filter=filters.market_filter(text_query="Soccer")
        )[0].event_type.id

        if args.command == "list":
            filter_teams = load_filter_teams(args.filter)
            list_todays_games(trading, soccer_event_type_id, filter_teams)
        elif args.command == "odds":
            event_name = " ".join(args.event_name)
            get_market_odds(
                trading, soccer_event_type_id, event_name, args.output_format
            )

    finally:
        trading.logout()
        print("Logged out.", file=sys.stderr)


if __name__ == "__main__":
    main()
