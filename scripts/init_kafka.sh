#!/bin/bash

# Nom du conteneur Kafka (Respecte la majuscule si ton conteneur s'appelle "Kafka")
CONTAINER_NAME="Kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "================================================="
echo "⏳  Attente du démarrage de Kafka..."
echo "================================================="

# On boucle tant que la commande de liste échoue
while ! docker exec $CONTAINER_NAME kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER > /dev/null 2>&1; do
  echo "   ... Kafka n'est pas encore prêt. Nouvelle tentative dans 2s..."
  sleep 2
done

echo "✅  Kafka est en ligne ! Création des topics..."
echo "-------------------------------------------------"

# Fonction pour créer un topic proprement
create_topic() {
  local topic_name=$1
  docker exec $CONTAINER_NAME kafka-topics --create \
    --if-not-exists \
    --topic "$topic_name" \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 1 \
    --replication-factor 1
  echo "   -> Topic '$topic_name' vérifié/créé."
}

# --- LISTE DE TES TOPICS ---
create_topic "topic_stock_prices"
create_topic "topic_reddit_posts"
create_topic "topic_stocktwits_posts"
create_topic "topic_yahoo_news"

echo "-------------------------------------------------"
echo "📜  Liste actuelle des topics dans Kafka :"
docker exec $CONTAINER_NAME kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
echo "================================================="
echo "🎉  Initialisation terminée !"