import requests
import pandas as pd
import time
import random

def scrape_reddit_loop(query="NVDA", subreddit="stocks", max_loops=5):
    all_posts = []
    after_token = None # Le curseur pour la page suivante
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"🚀 Démarrage du scraping sur r/{subreddit} pour '{query}'...")

    for i in range(max_loops):
        # On construit l'URL avec le paramètre 'after' si on l'a
        url = f"https://www.reddit.com/r/{subreddit}/search.json?q={query}&restrict_sr=1&sort=new&limit=100"
        if after_token:
            url += f"&after={after_token}"

        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"⚠️ Erreur {response.status_code}. Arrêt.")
                break
            
            data = response.json()
            children = data['data']['children']
            
            # Si plus de posts, on arrête
            if not children:
                print("Fin des résultats.")
                break

            # Extraction des données
            for post in children:
                p_data = post['data']
                all_posts.append({
                    'date': pd.to_datetime(p_data.get('created_utc'), unit='s'), # Conversion directe en date lisible
                    'titre': p_data.get('title'),
                    'texte': p_data.get('selftext'),
                    'score': p_data.get('score'),
                    'commentaires': p_data.get('num_comments'),
                    'subreddit': subreddit
                })
            
            # Mise à jour du token pour la page suivante
            after_token = data['data']['after']
            
            print(f"Batch {i+1}/{max_loops} récupéré. Total actuel: {len(all_posts)} posts.")
            
            if not after_token:
                break
                
            # PAUSE OBLIGATOIRE : Pour ne pas se faire bannir l'IP
            # On dort entre 2 et 5 secondes aléatoirement
            time.sleep(random.uniform(2, 5))

        except Exception as e:
            print(f"Erreur critique : {e}")
            break

    return pd.DataFrame(all_posts)

# --- UTILISATION ---
# On va chercher sur plusieurs subreddits pertinents
dfs = []
subreddits = ['wallstreetbets', 'stocks', 'investing', 'Nvidia']

for sub in subreddits:
    df_temp = scrape_reddit_loop(query="NVDA", subreddit=sub, max_loops=3) # 3 boucles * 100 posts approx
    dfs.append(df_temp)

# Fusionner tout en un seul grand tableau
final_df = pd.concat(dfs, ignore_index=True)

# Supprimer les doublons (même post dans plusieurs recherches)
final_df.drop_duplicates(subset=['titre'], inplace=True)
final_df.to_csv("nvda_reddit_posts.csv", index=False)

print(f"\n✅ Terminé ! {len(final_df)} posts uniques récupérés.")
print(final_df.head())