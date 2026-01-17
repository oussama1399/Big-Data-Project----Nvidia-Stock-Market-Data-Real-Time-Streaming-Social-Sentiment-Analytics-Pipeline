import yfinance as yf
import pandas as pd

# Télécharger les données historiques NVDA
nvda = yf.Ticker("NVDA")

df = nvda.history(
    period="1y",      # 1 an
    interval="1d"     # daily
)

# Reset index pour avoir la date comme colonne
df.reset_index(inplace=True)

# Convertir en JSON (liste d'objets)
json_data = df.to_json(
    "NVDA_historical_data.json",
    orient="records",
    date_format="iso"
)

print("JSON file generated: NVDA_historical_data.json")