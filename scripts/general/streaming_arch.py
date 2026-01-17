from graphviz import Digraph
import os

# Ajouter le chemin Graphviz au PATH système
graphviz_path = r'C:\Program Files\Graphviz\bin'
if os.path.exists(graphviz_path):
    os.environ['PATH'] += os.pathsep + graphviz_path
else:
    print(f"⚠️ Graphviz not found at {graphviz_path}")
    print("Please install Graphviz from: https://graphviz.org/download/")

# Configuration globale du graphe
dot = Digraph('NVDA_Stock_Stream_Analytics', filename='architecture_nvda_project', format='png')
dot.attr(rankdir='LR')  # Left to Right direction
dot.attr(splines='ortho') # Lignes orthogonales (plus propre)
dot.attr(nodesep='0.6', ranksep='1.0')

# Titre du diagramme
dot.attr(label=r'\nArchitecture Big Data: NVDA Stock Stream Analytics\nStack: Python | Kafka | Spark Streaming | MongoDB Atlas', 
         labelloc='t', fontsize='20', fontname="Arial-Bold")

# --- LAYER 1: DATA SOURCES (Local Python Scripts) ---
with dot.subgraph(name='cluster_0') as c:
    c.attr(style='filled', color='#e1f5fe', label='1. Ingestion Layer (Python Producers)')
    c.node_attr.update(style='filled', color='white', shape='note', fontname="Arial")
    
    c.node('P1', 'Stock Producer\n(yfinance)', color='#bbdefb')
    c.node('P2', 'Reddit Producer\n(PRAW)', color='#ffccbc')
    c.node('P3', 'StockTwits Producer\n(Requests)', color='#ffe0b2')
    c.node('P4', 'Yahoo News Producer\n(yfinance)', color='#d1c4e9')

# --- LAYER 2: BUFFERING (Kafka Cluster - Docker) ---
with dot.subgraph(name='cluster_1') as c:
    c.attr(style='filled', color='#fff3e0', label='2. Message Broker (Kafka - Docker)')
    c.node_attr.update(shape='box3d', style='filled', color='#ff9800', fontcolor='white', fontname="Arial-Bold")
    
    c.node('T1', 'Topic:\ntopic_stock_prices')
    c.node('T2', 'Topic:\ntopic_reddit_posts')
    c.node('T3', 'Topic:\ntopic_stocktwits_posts')
    c.node('T4', 'Topic:\ntopic_yahoo_news')

# --- LAYER 3: PROCESSING (Spark Streaming - Docker) ---
with dot.subgraph(name='cluster_2') as c:
    c.attr(style='filled', color='#fbe9e7', label='3. Stream Processing (Spark - Docker)')
    
    # Le moteur Spark
    c.node('Spark', 'Spark Structured\nStreaming\n(ETL & Cleaning)', 
           shape='component', style='filled', fillcolor='#ff5722', fontcolor='white', height='1.5')

# --- LAYER 4: STORAGE (MongoDB Atlas - Cloud) ---
with dot.subgraph(name='cluster_3') as c:
    c.attr(style='filled', color='#e8f5e9', label='4. Storage Layer (MongoDB Atlas)')
    c.node_attr.update(shape='cylinder', style='filled', fillcolor='#4caf50', fontcolor='white')
    
    c.node('DB_Stock', 'Collection:\nstock_prices\n(TimeSeries)')
    c.node('DB_Social', 'Collections:\n- nvidia_reddit_posts\n- nvidia_stocktwits_posts\n- nvidia_yahoo_news')

# --- LES CONNEXIONS (EDGES) ---

# 1. Scripts vers Kafka
dot.edge('P1', 'T1', label=' JSON')
dot.edge('P2', 'T2', label=' JSON')
dot.edge('P3', 'T3', label=' JSON')
dot.edge('P4', 'T4', label=' JSON')

# 2. Kafka vers Spark (Consuming)
dot.edge('T1', 'Spark')
dot.edge('T2', 'Spark')
dot.edge('T3', 'Spark')
dot.edge('T4', 'Spark', label=' Subscribe')

# 3. Spark vers MongoDB (Writing)
dot.edge('Spark', 'DB_Stock', label=' Write (Append)')
dot.edge('Spark', 'DB_Social', label=' Write (Append)')

# Générer le fichier
try:
    file_path = dot.render(view=True)
    print(f"✅ Diagramme généré avec succès : {file_path}")
except Exception as e:
    print(f"❌ Erreur : Assurez-vous d'avoir installé Graphviz sur votre OS.\n{e}")