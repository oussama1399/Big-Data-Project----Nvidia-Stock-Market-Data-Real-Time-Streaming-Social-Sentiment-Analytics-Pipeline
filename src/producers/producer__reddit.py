# src/producers/producer_reddit.py
import time
import json
import requests
import datetime
import sys
import os

# Ajout du chemin parent
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from kafka import KafkaProducer
# Si tu as un fichier config, décommente la ligne suivante :
# from src.config import kafka_config 

# --- CONFIGURATION ---
# Si tu utilises Docker, utilise 'kafka:29092', sinon 'localhost:9092'
BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC_NAME = 'topic_reddit_posts'
SUBREDDIT = "stocks"

# Initialisation Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0, 10, 1)
)

def stream_reddit():
    seen_ids = set()
    print(f"🚀 Streaming Reddit (r/{SUBREDDIT}) via Public JSON Scraping démarré...")

    # Headers indispensables pour imiter un vrai navigateur
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }

    while True:
        try:
            # L'extension .json agit comme une API non officielle
            url = f"https://www.reddit.com/r/{SUBREDDIT}/new.json?limit=10"
            
            response = requests.get(url, headers=headers, timeout=10)

            # Gestion des erreurs de blocage (Anti-Scraping)
            if response.status_code == 429:
                print("⚠️ Trop de requêtes (429). Pause de 60s...")
                time.sleep(60)
                continue
            elif response.status_code == 403:
                print("⛔ Accès interdit (403). Reddit bloque ton User-Agent.")
                time.sleep(60)
                continue
            elif response.status_code != 200:
                print(f"⚠️ Erreur HTTP {response.status_code}")
                time.sleep(10)
                continue

            # Parsing
            data = response.json().get('data', {}).get('children', [])
            
            count = 0
            for post in data:
                p_data = post['data']
                p_id = p_data['id']
                
                if p_id not in seen_ids:
                    # Conversion timestamp -> String date lisible
                    created_utc = p_data.get('created_utc')
                    date_str = datetime.datetime.fromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S')

                    post_json = {
                        'date': date_str,
                        'titre': p_data.get('title'),
                        'texte': p_data.get('selftext', '')[:500], # Limite la taille du texte
                        'score': p_data.get('score'),
                        'commentaires': p_data.get('num_comments'),
                        'subreddit': SUBREDDIT
                    }
                    
                    producer.send(TOPIC_NAME, value=post_json)
                    seen_ids.add(p_id)
                    count += 1
            
            if count > 0:
                print(f"✅ [Reddit] {count} nouveaux posts envoyés.")
                producer.flush()

            # Pause respectueuse (Reddit bannit si < 5-10s)
            time.sleep(30)

        except Exception as e:
            print(f"❌ Erreur Reddit: {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_reddit()