import pandas as pd

file_path = (
    "/home/james/bet_project/football/england_20_25/football_data_mixed_matches.csv"
)

df = pd.read_csv(file_path)

print(sorted(df["home_team"].unique()))
