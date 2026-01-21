# src/processors/spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
from urllib.parse import quote_plus
import logging

# Configuration du logging
logging.basicConfig(level=logging.ERROR)
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "Kafka:29092"

MONGO_USERNAME = "db_user_laghchimoussama"
MONGO_PASSWORD = "olmongodb171"
MONGO_CLUSTER = "ol-cluster.3agvwhk.mongodb.net"
MONGO_DATABASE = "NvidiaStockData"

encoded_username = quote_plus(MONGO_USERNAME)
encoded_password = quote_plus(MONGO_PASSWORD)

MONGO_URI = f"mongodb+srv://{encoded_username}:{encoded_password}@{MONGO_CLUSTER}/{MONGO_DATABASE}?retryWrites=true&w=majority"
DB_NAME = MONGO_DATABASE

# Noms des topics et collections
STREAMS_CONFIG = [
    {
        "topic": "topic_stock_prices",
        "collection": "stock_prices",
        "schema": StructType([
            StructField("Date", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Volume", LongType(), True),
            StructField("Dividends", DoubleType(), True),
            StructField("Stock Splits", DoubleType(), True)
        ]),
        "date_field": "Date"  # ← Champ qui contient la date
    },
    {
        "topic": "topic_reddit_posts",
        "collection": "nvidia_reddit_posts",
        "schema": StructType([
            StructField("date", StringType(), True),
            StructField("titre", StringType(), True),
            StructField("texte", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("commentaires", IntegerType(), True),
            StructField("subreddit", StringType(), True)
        ]),
        "date_field": "date"
    },
    {
        "topic": "topic_stocktwits_posts",
        "collection": "nvidia_stocktwits_posts",
        "schema": StructType([
            StructField("id", LongType(), True),
            StructField("date", StringType(), True),
            StructField("user", StringType(), True),
            StructField("text", StringType(), True),
            StructField("sentiment", StringType(), True),
            StructField("likes", IntegerType(), True)
        ]),
        "date_field": "date"
    },
    {
        "topic": "topic_yahoo_news",
        "collection": "nvidia_yahoo_news",
        "schema": StructType([
            StructField("publisher", StringType(), True),
            StructField("title", StringType(), True),
            StructField("link", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("pubdate", StringType(), True)
        ]),
        "date_field": "pubdate"
    }
]

# --- INITIALISATION SPARK ---
spark = SparkSession.builder \
    .appName("NVDA_Global_Stream_Processor") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", DB_NAME) \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- FONCTION DE PROCESSING ---
def run_stream_pipeline(config):
    topic = config["topic"]
    collection = config["collection"]
    schema = config["schema"]
    date_field = config.get("date_field")

    print(f"🔌 Connexion au flux : {topic} -> Collection : {collection}")

    # 1. Lecture Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parsing JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Ajouter un champ timestamp pour MongoDB
    if date_field and date_field in df_parsed.columns:
        # Convertir le champ date existant en timestamp
        df_with_timestamp = df_parsed.withColumn(
            "timestamp", 
            to_timestamp(col(date_field))
        )
    else:
        # Ajouter le timestamp actuel si pas de champ date
        df_with_timestamp = df_parsed.withColumn(
            "timestamp", 
            current_timestamp()
        )

    # 4. Écriture Mongo
    query = df_with_timestamp.writeStream \
            .format("mongodb") \
            .option("checkpointLocation", f"/tmp/checkpoints/{collection}") \
            .option("forceDeleteTempCheckpointLocation", "true") \
            .option("spark.mongodb.connection.uri", MONGO_URI) \
            .option("spark.mongodb.database", DB_NAME) \
            .option("spark.mongodb.collection", collection) \
            .outputMode("append") \
            .start()
    
    return query

# --- LANCEMENT DES 4 STREAMS ---
active_streams = []
for stream_conf in STREAMS_CONFIG:
    q = run_stream_pipeline(stream_conf)
    active_streams.append(q)

print(f"🚀 {len(active_streams)} Flux Spark sont actifs et écoutent Kafka...")

# Garder le processus en vie
spark.streams.awaitAnyTermination()