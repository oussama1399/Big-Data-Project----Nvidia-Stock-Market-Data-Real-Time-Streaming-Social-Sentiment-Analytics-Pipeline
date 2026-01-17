import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def stream_reddit():
    seen_ids = set()
    subreddit = "stocks"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    print("🚀 Streaming Reddit Started...")
    while True:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=5"
            resp = requests.get(url, headers=headers)
            data = resp.json()['data']['children']
            
            for post in data:
                p_data = post['data']
                p_id = p_data['id']
                
                if p_id not in seen_ids:
                    # SCHEMA STRICT RESPECTÉ
                    post_json = {
                        'date': str(pd.to_datetime(p_data.get('created_utc'), unit='s')),
                        'titre': p_data.get('title'),
                        'texte': p_data.get('selftext'),
                        'score': p_data.get('score'),
                        'commentaires': p_data.get('num_comments'),
                        'subreddit': subreddit
                    }
                    
                    producer.send('topic_reddit_posts', value=post_json)
                    print(f"✅ Sent Reddit: {post_json['titre'][:20]}...")
                    seen_ids.add(p_id)
            
            producer.flush()
            time.sleep(30) # Check toutes les 30 sec
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_reddit()