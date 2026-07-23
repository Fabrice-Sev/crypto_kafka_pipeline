import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# Define schema matching Producer output
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_id", LongType(), True)
])

def start_spark_stream():
    # Maven packages for Kafka & MongoDB Spark Connectors
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    ]
    
    # 1. Initialize Spark Session with MongoDB configurations
    spark = SparkSession.builder \
        .appName("CryptoSparkStreaming") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.database", MONGO_DB) \
        .config("spark.mongodb.write.collection", MONGO_COLLECTION) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Engine Initialized...")

    # 2. Read Micro-batches from Kafka
    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON strings into structured columns
    parsed_df = kafka_raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            # Convert millisecond epoch into Timestamp
            (col("data.timestamp") / 1000).cast(TimestampType()).alias("timestamp"),
            col("data.price").alias("price"),
            col("data.quantity").alias("quantity"),
            col("data.trade_id").alias("trade_id")
        )

    # 4. Stream transformed micro-batches directly to MongoDB
    query = parsed_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoint") \
        .outputMode("append") \
        .start()

    print("Spark Streaming to MongoDB active...")
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_stream()