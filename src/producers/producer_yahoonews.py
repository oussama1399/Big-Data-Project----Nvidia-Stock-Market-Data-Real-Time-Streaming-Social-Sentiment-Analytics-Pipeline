import time
import json
import yfinance as yf
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def stream_yahoo_news():
    seen_links = set()
    print("🚀 Streaming Yahoo News Started...")
    
    while True:
        try:
            nvda = yf.Ticker("NVDA")
            news_list = nvda.news
            
            for item in news_list:
                link = item.get('content', {}).get('canonicalUrl', {}).get('url')
                
                if link and link not in seen_links:
                    # SCHEMA STRICT RESPECTÉ
                    news_json = {
                        'publisher': item.get('content', {}).get('provider', {}).get('displayName'),
                        'title': item.get('content', {}).get('title'),
                        'link': link,
                        'summary': item.get('content', {}).get('summary'),
                        'pubdate': item.get('content', {}).get('pubDate')
                    }
                    
                    producer.send('topic_yahoo_news', value=news_json)
                    print(f"✅ Sent News: {news_json['title'][:20]}...")
                    seen_links.add(link)
            
            time.sleep(300) # Check news every 5 mins
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_yahoo_news()