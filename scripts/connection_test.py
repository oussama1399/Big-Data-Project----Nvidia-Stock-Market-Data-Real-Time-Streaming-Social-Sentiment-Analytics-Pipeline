# Testez cette connexion Python d'abord
from pymongo import MongoClient
from urllib.parse import quote_plus

username = quote_plus("##############")
password = quote_plus("##########")
uri = f"mongodb+srv://{username}:{password}@ol-cluster.3agvwhk.mongodb.net/NvidiaStockData?retryWrites=true&w=majority"

try:
    client = MongoClient(uri)
    print("✅ Connexion MongoDB réussie!")
    print("Databases:", client.list_database_names())
except Exception as e:
    print(f"❌ Erreur: {e}")
