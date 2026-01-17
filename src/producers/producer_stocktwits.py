# src/producers/stocktwits_producer.py
import time
import json
import requests
import sys
import os

# Ajout du chemin parent pour importer la config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from kafka import KafkaProducer
from src.config import kafka_config

# Initialisation Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_config.BOOTSTRAP_SERVERS_LOCAL,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def stream_stocktwits():
    symbol = "NVDA"
    seen_ids = set()
    print(f"🚀 Streaming StockTwits pour {symbol} démarré...")

    while True:
        try:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 429:
                print("⚠️ Rate Limit StockTwits. Pause 60s...")
                time.sleep(60)
                continue
                
            data = response.json()
            messages = data.get('messages', [])

            count = 0
            for msg in messages:
                msg_id = msg['id']
                
                if msg_id not in seen_ids:
                    # Extraction du sentiment (parfois null)
                    sentiment = None
                    if msg.get('entities') and msg['entities'].get('sentiment'):
                        sentiment = msg['entities']['sentiment']['basic']

                    # --- SCHÉMA STRICT ---
                    twit_json = {
                        'id': msg_id,
                        'date': msg['created_at'], # Format ISO fourni par l'API
                        'user': msg['user']['username'],
                        'text': msg['body'],
                        'sentiment': sentiment,
                        'likes': msg.get('likes', {}).get('total', 0)
                    }

                    # Envoi Kafka
                    producer.send(kafka_config.TOPIC_STOCKTWITS, value=twit_json)
                    seen_ids.add(msg_id)
                    count += 1

            if count > 0:
                print(f"✅ [StockTwits] {count} nouveaux messages envoyés.")
            
            # StockTwits bloque si on requête trop vite. 15-20 secondes est safe.
            time.sleep(20)

        except Exception as e:
            print(f"❌ Erreur StockTwits : {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_stocktwits()