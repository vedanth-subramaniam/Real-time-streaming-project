from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Create a Spark session
spark = SparkSession.builder \
    .appName("RedditDataConsumer") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("Title", StringType(), True),
    StructField("comments", ArrayType(StructType([
        StructField("Body", StringType(), True)
    ])), True)
])

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data
df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write data to console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
