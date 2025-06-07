import os
os.environ["CUDA_VISIBLE_DEVICES"] = ""  # Disable GPU
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"  # Disable oneDNN
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"  # Suppress TensorFlow warnings

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
from transformers import pipeline
import pandas as pd
import logging
from datetime import datetime, timezone, time
from database_storage import NewsDB
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
output_kafka_topic = "news-summary"
mongodb_uri = os.getenv("MONGODB_URI")
if not mongodb_uri:
    logger.error("MONGODB_URI environment variable not set")
    raise ValueError("MONGODB_URI environment variable not set")

spark = SparkSession.builder \
    .appName("NewsBatchSummarizer") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.python.worker.log.level", "DEBUG") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark session initialized")

def summarize_batch(iterator):
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=-1)
    logger.info("Summarizer initialized in worker")

    for pdf in iterator:
        logger.info("Processing batch with %d records", len(pdf))
        results = []
        for _, row in pdf.iterrows():
            content = row["content"]
            category = row["category"]
            if not isinstance(content, str) or not content.strip():
                logger.warning("Empty or non-string content detected: %s", content)
                results.append({
                    "summary": "",
                    "category": category
                })
                continue
            try:
                input_length = len(content.split())
                max_length = min(150, max(30, input_length // 2))
                summary = summarizer(content, max_length=max_length, min_length=30, do_sample=False)[0]["summary_text"]
                results.append({
                    "summary": summary,
                    "category": category
                })
            except Exception as e:
                logger.error("Error summarizing content '%s': %s", content[:50], str(e))
                results.append({
                    "summary": "",
                    "category": category
                })
        logger.info("Batch summarization completed")
        yield pd.DataFrame(results)

try:
    news_db = NewsDB()
    fixed_time = datetime.combine(datetime.now(timezone.utc).date(), time(13, 30, tzinfo=timezone.utc))
    news_docs = news_db.find_after(fixed_time)
    logger.info("Fetched %d news document from MongoDB after %s", len(news_docs), fixed_time)

    if not news_docs:
        logger.info("No news documents found after %s", fixed_time)
        spark.stop()
        exit(0)

    schema = StructType([
        StructField("content", StringType(), True),
        StructField("category", StringType(), True)
    ])

    news_data = [{"content": doc["content"], "category": doc["category"]} for doc in news_docs]
    df = spark.createDataFrame(news_data, schema=schema).repartition(1)

    summary_schema = "summary string, category string"
    df_summarized = df.mapInPandas(summarize_batch, schema=summary_schema)

    df_kafka = df_summarized.select(
        to_json(struct(
            col("summary").alias("content").cast("string"),
            col("category").cast("string")
        )).alias("value")
    )

    logger.info("Kafka DataFrame schema:")
    df_kafka.printSchema()

    try:
        df_kafka.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", output_kafka_topic) \
            .option("failOnDataLoss", "false") \
            .save()
        logger.info("Summarized news written to Kafka topic: %s", output_kafka_topic)
    except Exception as e:
        logger.error("Failed to write to Kafka: %s", str(e))
        raise

except Exception as e:
    logger.error("Error in batch processing: %s", str(e))
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")