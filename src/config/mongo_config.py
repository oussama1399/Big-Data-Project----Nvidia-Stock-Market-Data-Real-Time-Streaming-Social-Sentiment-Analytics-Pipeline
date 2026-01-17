# src/config/mongo_config.py

# MongoDB Configuration
USERNAME = "db_user_laghchimoussama@admin"
PASSWORD = "############"
CLUSTER_URL = "ol-cluster.3agvwhk.mongodb.net"

MONGO_URI = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_URL}/?retryWrites=true&w=majority"

# Database
DB_NAME = "NvidiaStockData"

# Collections
COL_STOCK = "stock_prices"
COL_REDDIT = "nvidia_reddit_posts"
COL_STOCKTWITS = "nvidia_stocktwits_posts"
COL_NEWS = "nvidia_yahoo_news"