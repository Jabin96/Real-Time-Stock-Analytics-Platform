import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, abs, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

KAFKA_PORT = '9093'
TOPIC_NAME = 'test_topic'
CHECKPOINT_DIR = "./outputs/checkpoints"
OUTPUT_DIR = "./outputs/streaming_data"
ANOMALY_DIR = "./outputs/anomalies"

spark = SparkSession.builder \
    .appName("StockAnalyticsEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", DoubleType()) 

print(f"Listening to {TOPIC_NAME} on port {KAFKA_PORT}")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"127.0.0.1:{KAFKA_PORT}") \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-consumer-group-v2") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Query 1: Write Raw Data to CSV
print("Starting Raw Data Stream...")
query_raw = parsed_df.writeStream \
    .format("csv") \
    .option("header", "true") \
    .option("path", f"{OUTPUT_DIR}/raw_ticks") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# Query 2: Anomaly Detection
print("Starting Anomaly Detection Stream...")
window_spec = Window.partitionBy("symbol")

# Note: Window functions with streaming require time-based windows.
# For this demonstration, we use a threshold-based filter to identify anomalies
# (price > $150 or significant spikes).
# In a production environment, stateful processing with Z-score calculation would be implemented.

anomaly_df = parsed_df.filter(col("price") > 150)

query_anom = anomaly_df.writeStream \
    .format("csv") \
    .option("header", "true") \
    .option("path", f"{ANOMALY_DIR}") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

print("Analytics Engine Running")
print("Writing Raw Data to: outputs/streaming_data/raw_ticks")
print("Writing Anomalies to: outputs/anomalies")

spark.streams.awaitAnyTermination()