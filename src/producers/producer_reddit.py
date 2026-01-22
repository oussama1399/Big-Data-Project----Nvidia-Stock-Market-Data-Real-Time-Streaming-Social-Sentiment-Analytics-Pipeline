import requests
import pandas as pd
import time
import random
import json
from kafka import KafkaProducer

# 1. Configuration Kafka
TOPIC_NAME = 'topic_reddit_posts'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# To avoid duplicates during this session
seen_ids = set()

def scrape_reddit_to_kafka(query="NVDA", subreddit="stocks", max_loops=5):
    after_token = None 
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"🚀 Streaming from r/{subreddit} for query '{query}'...")

    for i in range(max_loops):
        # Build URL with pagination
        url = f"https://www.reddit.com/r/{subreddit}/search.json?q={query}&restrict_sr=1&sort=new&limit=100"
        if after_token:
            url += f"&after={after_token}"

        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"⚠️ Error {response.status_code}. Skipping batch.")
                break
            
            data = response.json()
            children = data.get('data', {}).get('children', [])
            
            if not children:
                print("No more results.")
                break

            # Process posts
            count_sent = 0
            for post in children:
                p_data = post['data']
                p_id = p_data.get('id')

                # 2. CHECK DUPLICATES
                if p_id not in seen_ids:
                    # Create clean JSON object
                    post_json = {
                        'id': p_id,
                        'date': str(pd.to_datetime(p_data.get('created_utc'), unit='s')), # FIX: Convert to string for JSON
                        'titre': p_data.get('title'),
                        'texte': p_data.get('selftext'),
                        'score': p_data.get('score'),
                        'commentaires': p_data.get('num_comments'),
                        'subreddit': subreddit,
                        'query': query
                    }
                    
                    # 3. SEND TO KAFKA
                    producer.send(TOPIC_NAME, value=post_json)
                    seen_ids.add(p_id)
                    count_sent += 1
            
            producer.flush()
            print(f"   -> Batch {i+1}: Sent {count_sent} new posts to Kafka.")
            
            # Update token for next page
            after_token = data['data']['after']
            
            if not after_token:
                break
                
            # Anti-ban pause
            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"Critical Error: {e}")
            break

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    subreddits = ['wallstreetbets', 'stocks', 'investing', 'Nvidia']
    
    # Infinite loop to keep the producer alive (Optional: remove 'while True' if you only want to run once)
    while True:
        print("\n--- Starting Cycle ---")
        for sub in subreddits:
            scrape_reddit_to_kafka(query="NVDA", subreddit=sub, max_loops=1)
        
        print("✅ Cycle finished. Sleeping 60 seconds...")
        time.sleep(60)