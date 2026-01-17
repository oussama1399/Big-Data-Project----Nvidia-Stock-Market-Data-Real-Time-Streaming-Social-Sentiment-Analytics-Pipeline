# src/producers/producer_stocktwits.py
import time
import json
import requests
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from kafka import KafkaProducer
# from src.config import kafka_config # Décommenter si utilisé

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'topic_stocktwits_posts'
SYMBOL = "NVDA"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 10, 1)
)

def stream_stocktwits():
    seen_ids = set()
    print(f"🚀 Streaming StockTwits pour {SYMBOL} (Scraping mode)...")

    # Headers renforcés
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://stocktwits.com/',
        'Origin': 'https://stocktwits.com'
    }

    while True:
        try:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{SYMBOL}.json"
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 429:
                print("⚠️ Rate Limit StockTwits. Pause 60s...")
                time.sleep(60)
                continue
            elif response.status_code != 200:
                print(f"⚠️ Erreur StockTwits: {response.status_code}")
                time.sleep(20)
                continue
                
            data = response.json()
            messages = data.get('messages', [])

            count = 0
            for msg in messages:
                msg_id = msg['id']
                
                if msg_id not in seen_ids:
                    sentiment = None
                    if msg.get('entities') and msg['entities'].get('sentiment'):
                        sentiment = msg['entities']['sentiment']['basic']

                    twit_json = {
                        'id': msg_id,
                        'date': msg['created_at'],
                        'user': msg['user']['username'],
                        'text': msg['body'],
                        'sentiment': sentiment,
                        'likes': msg.get('likes', {}).get('total', 0)
                    }

                    producer.send(TOPIC_NAME, value=twit_json)
                    seen_ids.add(msg_id)
                    count += 1

            if count > 0:
                print(f"✅ [StockTwits] {count} messages envoyés.")
                producer.flush()
            
            time.sleep(20)

        except Exception as e:
            print(f"❌ Crash StockTwits : {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_stocktwits()