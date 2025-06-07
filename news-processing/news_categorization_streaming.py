from pyspark.sql import SparkSession
from transformers import pipeline
import pandas as pd
import os
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

class NewsDB:
    def __init__(self, uri, db_name="mydatabase"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.news_collection = self.db["News"]

    def insert_news(self, content, category):
        document = {
            "content": content,
            "category": category,
            "created_at": datetime.now(timezone.utc)
        }
        result = self.news_collection.insert_one(document)
        return result.inserted_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = "general-news-2"
checkpoint_location = f"/tmp/spark_checkpoint_{kafka_topic}_{int(datetime.now().timestamp())}"
mongodb_uri = os.getenv("MONGODB_URI")

spark = SparkSession.builder \
    .appName("KafkaNewsClassifier") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session initialized")

try:
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info("Connected to Kafka topic: %s", kafka_topic)
except Exception as e:
    logger.error("Failed to connect to Kafka: %s", str(e))
    raise

df_decoded = df_raw.selectExpr("CAST(value AS STRING) as message")

categories = ["environmental news", "health news", "technology", "political", "arts", "sports", "social"]

def classify_batch(iterator):
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli", device=-1)
    logger.info("Classifier initialized in worker")
    news_db = NewsDB(uri=mongodb_uri)

    for pdf in iterator:
        logger.info("Processing batch with %d records", len(pdf))
        results = []
        if "message" not in pdf.columns:
            logger.error("Column 'message' not found in DataFrame")
            yield pd.DataFrame({"message": [], "category": [], "confidence": []})
            continue

        for msg in pdf["message"]:
            if not isinstance(msg, str):
                logger.warning("Non-string message detected: %s", msg)
                results.append({
                    "message": str(msg),
                    "category": "unknown",
                    "confidence": 0.0
                })
                continue
            try:
                result = classifier(msg, candidate_labels=categories, multi_label=False)
                category = result["labels"][0]
                confidence = float(result["scores"][0])

                try:
                    news_db.insert_news(content=msg, category=category)
                except Exception as db_e:
                    logger.error(f"MongoDB insert failed for message: {msg}, error: {db_e}")

                results.append({
                    "message": msg,
                    "category": category,
                    "confidence": confidence
                })
            except Exception as e:
                logger.error("Error processing message '%s': %s", msg, str(e))
                results.append({
                    "message": msg,
                    "category": "unknown",
                    "confidence": 0.0
                })
        logger.info("Batch processing completed")
        yield pd.DataFrame(results)

schema = "message string, category string, confidence double"
df_classified = df_decoded.mapInPandas(
    classify_batch,
    schema=schema
)

query = df_classified.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Received shutdown signal")
    query.stop()
    spark.stop()
    logger.info("Spark session stopped gracefully")