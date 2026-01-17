import requests
import pandas as pd
import time
from datetime import datetime

def get_stocktwits_historical(symbol="NVDA", since_date="2024-01-01"):
    """
    Récupère les messages StockTwits en remontant le temps jusqu'à 'since_date'.
    """
    # Conversion de la date limite en format datetime
    target_date = pd.to_datetime(since_date).tz_localize('UTC')
    
    all_messages = []
    cursor = None # Le paramètre 'max' pour la pagination
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    print(f"🔄 Démarrage du scraping pour {symbol} jusqu'au {since_date}...")
    
    while True:
        url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
        
        # On ajoute le curseur (max) si on a déjà fait un tour de boucle
        params = {}
        if cursor:
            params['max'] = cursor
            
        try:
            r = requests.get(url, headers=headers, params=params)
            
            if r.status_code == 429:
                print("⚠️ Trop de requêtes (Rate Limit). Pause de 60 sec...")
                time.sleep(60)
                continue
            elif r.status_code != 200:
                print(f"Erreur API: {r.status_code}")
                break
                
            data = r.json()
            messages = data.get('messages', [])
            
            if not messages:
                print("Fin du flux (plus de messages).")
                break
                
            # Traitement des messages
            batch_dates = []
            for msg in messages:
                msg_date = pd.to_datetime(msg['created_at'])
                
                # Si le message est plus vieux que notre cible, on note
                if msg_date < target_date:
                    pass # On continuera juste pour finir le batch, mais la boucle s'arrêtera
                
                # Extraction simplifiée
                sentiment = msg['entities']['sentiment']['basic'] if msg['entities']['sentiment'] else None
                
                all_messages.append({
                    'id': msg['id'],
                    'date': msg_date,
                    'user': msg['user']['username'],
                    'text': msg['body'],
                    'sentiment': sentiment,
                    'likes': msg.get('likes', {}).get('total', 0)
                })
                batch_dates.append(msg_date)
            
            # --- CONDITION D'ARRÊT ---
            # Si le message le plus vieux de ce lot est PLUS VIEUX que notre target_date
            last_msg_date = batch_dates[-1]
            if last_msg_date < target_date:
                print(f"🎯 Date cible atteinte ({last_msg_date}). Arrêt.")
                break
                
            # Préparer le curseur pour le prochain tour (ID du dernier message - 1)
            cursor = messages[-1]['id']
            
            print(f"Batch récupéré. Dernier: {last_msg_date}. Total: {len(all_messages)} msgs.")
            
            # PAUSE IMPORTANTE : StockTwits bloque si on va trop vite
            time.sleep(1.5) 
            
        except Exception as e:
            print(f"Erreur critique : {e}")
            break
            
    # Conversion en DataFrame et nettoyage final
    df = pd.DataFrame(all_messages)
    
    # On coupe tout ce qui dépasse strictement la date cible (nettoyage précis)
    if not df.empty:
        df = df[df['date'] >= target_date]
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        df.to_csv(f"stocktwits_{symbol}_messages_period-{since_date}.csv", index=False)
    return df

# --- UTILISATION ---
# Exemple : Récupérer les messages des 3 derniers jours
import datetime
last_three_days = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')

df_result = get_stocktwits_historical("NVDA", since_date=last_three_days)

print(f"\n✅ Terminé ! {len(df_result)} messages récupérés.")
print(df_result[['date', 'sentiment', 'text']].head())