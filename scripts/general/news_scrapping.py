import yfinance as yf
import pandas as pd

# 1. Définir le ticker (NVIDIA)
nvda = yf.Ticker("NVDA")

# 2. Récupérer les news
# Cela renvoie une liste de dictionnaires (JSON)
news_list = nvda.news

# 3. Afficher les titres pour ton analyse de sentiment
print(f"Dernières nouvelles pour NVIDIA ({len(news_list)} trouvées) :\n")
df=[]
for item in news_list:
    # print(item)  # Affiche le dictionnaire complet pour inspection
    # print(item['content'].keys())
    title = item.get('content', {}).get('title')
    publisher = item.get('content', {}).get('provider', {}).get('displayName')
    link = item.get('content', {}).get('canonicalUrl', {}).get('url')
    summary = item.get('content', {}).get('summary')
    pubdate = item.get('content', {}).get('pubDate')
    df.append({
        'publisher': publisher,
        'title': title,
        'link': link,
        'summary': summary,
        'pubdate': pubdate
    })
    # print(f"source: {publisher}")
    # print(f"Titre: {title}")
    # print(f"Lien: {link}")
    # print(f"Summary: {summary}")
    # print(f"Publish date: {pubdate}")
    # print("-" * 30)
df=pd.DataFrame(df)
df.to_csv("nvda_yfinance_news.csv", index=False)
print("Data saved to 'nvda_yfinance_news.csv'")
