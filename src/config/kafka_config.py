# src/config/kafka_config.py

# Adresses du serveur Kafka
# Pour tes scripts Python sur Windows
BOOTSTRAP_SERVERS_LOCAL = ['localhost:9092']
# Pour Spark à l'intérieur du conteneur Docker
BOOTSTRAP_SERVERS_DOCKER = 'kafka:29092'

# Liste des Topics
TOPIC_STOCK = 'topic_stock_prices'
TOPIC_REDDIT = 'topic_reddit_posts'
TOPIC_STOCKTWITS = 'topic_stocktwits_posts'
TOPIC_NEWS = 'topic_yahoo_news'