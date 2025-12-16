from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
import os

# ==============================
# Configuration Hadoop / Windows
# ==============================
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] += ";C:\\hadoop\\bin"

# Chemins
checkpoint_console = r"C:\tmp\spark_checkpoints_console"
checkpoint_parquet = r"C:\tmp\spark_checkpoints_parquet"
output_dir = r"C:\tmp\spark_parquet"

os.makedirs(checkpoint_console, exist_ok=True)
os.makedirs(checkpoint_parquet, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# =============================
# Initialiser Spark
# =============================
spark = SparkSession.builder \
    .appName("KafkaCryptoStream") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================
# Schéma JSON
# =============================
schema = StructType() \
    .add("symbol", StringType()) \
    .add("close_price", DoubleType()) \
    .add("open_price", DoubleType()) \
    .add("high_price", DoubleType()) \
    .add("low_price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("quote_volume", DoubleType()) \
    .add("timestamp", LongType())

# =============================
# Lecture Kafka
# =============================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "projet-bdcc") \
    .option("startingOffsets", "latest") \
    .load()

# Transformer JSON en colonnes
json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# =============================
# STREAM 1 : afficher dans la console
# =============================
console_query = json_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", checkpoint_console) \
    .start()

# =============================
# STREAM 2 : écrire en Parquet pour Streamlit
# =============================
parquet_query = json_df.writeStream \
    .format("parquet") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_parquet) \
    .outputMode("append") \
    .start()

console_query.awaitTermination()
parquet_query.awaitTermination()
