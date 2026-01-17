import time
import json
import yfinance as yf
from kafka import KafkaProducer

# Initialisation du Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 10, 1)
)

def stream_stock_prices():
    print("🚀 Streaming Stock Data Started...")
    while True:
        try:
            # Récupérer la dernière minute
            nvda = yf.Ticker("NVDA")
            df = nvda.history(period="1d", interval="1m")
            
            if not df.empty:
                latest = df.iloc[-1]
                
                # SCHEMA STRICT RESPECTÉ
                stock_json = {
                    "Date": str(latest.name), # Conversion Timestamp -> String
                    "Open": latest["Open"],
                    "High": latest["High"],
                    "Low": latest["Low"],
                    "Close": latest["Close"],
                    "Volume": int(latest["Volume"]),
                    "Dividends": latest["Dividends"],
                    "Stock Splits": latest["Stock Splits"]
                }
                
                producer.send('topic_stock_prices', value=stock_json)
                print(f"✅ Sent Stock: {stock_json['Close']} at {stock_json['Date']}")
            
            time.sleep(60) # Pause d'une minute
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    stream_stock_prices()