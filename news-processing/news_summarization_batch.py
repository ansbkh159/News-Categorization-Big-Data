import os
os.environ["CUDA_VISIBLE_DEVICES"] = ""
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col, concat_ws, collect_list, lit, concat
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

try:
    summarizer = pipeline("summarization", model="t5-small", device=-1, batch_size=1)
    logger.info("Global summarizer initialized on CPU with batch_size=1")
except Exception as e:
    logger.error("Failed to initialize summarizer: %s", str(e))
    raise

num_partitions = 2

spark = SparkSession.builder \
    .appName("NewsBatchSummarizer") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.python.worker.log.level", "DEBUG") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", str(num_partitions)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logger.info("Spark session initialized with %d partitions", num_partitions)

def summarize_batch(iterator):
    for pdf in iterator:
        results = []
        batch_size = 1
        for i in range(0, len(pdf), batch_size):
            batch = pdf[i:i + batch_size]
            for idx, row in batch.iterrows():
                content = row["content"]
                category = row["category"]
                if not isinstance(content, str) or not content.strip():
                    results.append({"summary": "", "category": category})
                    continue
                try:
                    content = content[:5000]
                    input_length = len(content.split())
                    max_length = min(100, max(20, input_length // 3))
                    summary = summarizer(content, max_length=max_length, min_length=20, do_sample=False)[0]["summary_text"]
                    results.append({"summary": summary, "category": category})
                except Exception as e:
                    logger.error("Error summarizing row %d: %s", idx, str(e))
                    results.append({"summary": "", "category": category})
        yield pd.DataFrame(results)

try:
    news_db = NewsDB()
    fixed_time = datetime.combine(datetime.now(timezone.utc).date(), time(13, 30, tzinfo=timezone.utc))
    news_docs = news_db.find_after(fixed_time)
    logger.info("Fetched %d news documents from MongoDB", len(news_docs))

    if not news_docs:
        logger.info("No news documents found after %s", fixed_time)
        spark.stop()
        exit(0)

    schema = StructType([
        StructField("content", StringType(), True),
        StructField("category", StringType(), True)
    ])

    max_content_length = 5000
    news_data = [{"content": doc["content"][:max_content_length] if isinstance(doc["content"], str) else "", 
                  "category": doc["category"]} for doc in news_docs]
    df = spark.createDataFrame(news_data, schema=schema).repartition(num_partitions)

    summary_schema = StructType([
        StructField("summary", StringType(), True),
        StructField("category", StringType(), True)
    ])
    df_summarized = df.mapInPandas(summarize_batch, schema=summary_schema)

    df_with_bullets = df_summarized.filter(col("summary") != "").withColumn(
        "summary", concat(lit("- "), col("summary"))
    )
    df_grouped = df_with_bullets.groupBy("category").agg(
        concat_ws("\n", collect_list("summary")).alias("content")
    )

    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df_grouped = df_grouped.withColumn(
        "content", concat(col("content"), lit(f"\nDate: {today_date}"))
    )

    df_kafka = df_grouped.select(
        to_json(struct(
            col("content").cast("string"),
            col("category").cast("string")
        )).alias("value")
    )

    df_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("failOnDataLoss", "false") \
        .option("kafka.create.topics", "true") \
        .save()
    logger.info("Summarized news grouped by category with bullet points and date, written to Kafka topic: %s", output_kafka_topic)

except Exception as e:
    logger.error("Error in batch processing: %s", str(e))
    raise

finally:
    spark.stop()
    logger.info("Spark session stopped")



## RUN WITH 
# spark-submit \
#   --master local[4] \
#   --driver-memory 2g \
#   --executor-memory 1g \
#   --executor-cores 1 \
#   --num-executors 1 \
#   --conf spark.dynamicAllocation.enabled=false \
#   --conf spark.sql.execution.arrow.pyspark.enabled=true \
#   --conf spark.shuffle.service.enabled=true \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
#   --py-files database_storage.py \
#   news_summarization_batch.py
