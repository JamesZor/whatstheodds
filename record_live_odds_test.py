import json
import subprocess
from datetime import datetime

# MATCHES_TO_TRACK = {
#     "Drogheda v St Patricks": "drogheda-united",
#     "Dundalk v Galway Utd": "dundalk-fc",
#     "Shelbourne v Derry City": "shelbourne",
#     "Shamrock Rovers v Bohemians": "shamrock-rovers",
# }
#
# JSONL_FILENAME = "raw_odds_history.jsonl"
#


MATCHES_TO_TRACK = {
    "Alloa v East Fife": "alloa-athletic",
    "Cove Rangers v Stenhousemuir": "cove-rangers",
    "Inverness CT v Kelty Hearts": "inverness-caledonian-thistle",
    "Montrose v Hamilton": "montrose",
    "Peterhead v Queen of South": "peterhead",
    "Annan v Spartans": "annan-athletic",
    "Clyde v Dumbarton": "clyde-fc",
    "East Kilbride v Edinburgh City": "east-kilbride",
    "Stirling v Elgin City FC": "stirling-albion",
    "Stranraer v Forfar": "stranraer",
}

JSONL_FILENAME = "raw_odds_history_scotland.jsonl"


def fetch_and_append_json():
    iso_timestamp = datetime.now().isoformat()
    print(f"[{iso_timestamp}] Fetching full JSON payloads...")

    for bf_name, db_name in MATCHES_TO_TRACK.items():
        try:
            # Run the CLI tool
            cmd = ["python", "live_odds_cli.py", "odds", bf_name, "-d"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Isolate the JSON string from the CLI text output
            output_lines = result.stdout.splitlines()
            json_start = next(
                (i for i, line in enumerate(output_lines) if line.startswith("{")), None
            )

            if json_start is not None:
                # Parse to ensure it's valid JSON
                raw_json_data = json.loads("\n".join(output_lines[json_start:]))

                # Build the complete payload
                payload = {
                    "timestamp": iso_timestamp,
                    "home_team": db_name,
                    "betfair_match_name": bf_name,
                    "market_data": raw_json_data,
                }

                # Append as a single JSON string with a newline
                with open(JSONL_FILENAME, "a") as f:
                    f.write(json.dumps(payload) + "\n")

                print(
                    f"  ✅ Appended full JSON payload for {db_name} to {JSONL_FILENAME}"
                )
            else:
                print(f"  ⚠️ No JSON found in output for {bf_name}")

        except Exception as e:
            print(f"  ❌ Error fetching {bf_name}: {e}")


if __name__ == "__main__":
    fetch_and_append_json()
