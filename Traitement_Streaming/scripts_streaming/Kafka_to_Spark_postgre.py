from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os
import logging


# Logging Python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


# Configuration Hadoop (Windows)
if os.name == "nt":
    os.environ['HADOOP_HOME'] = "C:\\hadoop"
    os.environ['PATH'] += ";C:\\hadoop\\bin"


# Kafka Configuration
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "projet-bdcc")


# SparkSession
spark = SparkSession.builder \
    .appName("ProjetCrypto") \
    .master("local[*]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logging.info("SparkSession créée avec succès.")


# Schéma JSON Binance
schema = StructType() \
    .add("symbol", StringType()) \
    .add("close_price", DoubleType()) \
    .add("open_price", DoubleType()) \
    .add("high_price", DoubleType()) \
    .add("low_price", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("quote_volume", DoubleType()) \
    .add("timestamp", DoubleType())   # reçu en millisecondes


# Lecture Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()


# Transformation JSON et timestamp

json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Conversion du timestamp en TimestampType exploitable
json_df = json_df.withColumn("timestamp_ts", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))

# Colonnes calculées
json_df = json_df.withColumn("spread", col("high_price") - col("low_price")) \
                 .withColumn("mid_price", (col("high_price") + col("low_price")) / 2)


# Fonction pour écrire dans PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        # Supprimer les doublons sur les colonnes clés (timestamp + symbol)
        batch_df = batch_df.dropDuplicates(["timestamp_ts", "symbol"])

        # Sélection des colonnes utiles
        cols_to_write = [
            "symbol", "close_price", "open_price", "high_price", "low_price",
            "volume", "quote_volume", "timestamp_ts", "spread", "mid_price"
        ]
        batch_df_to_write = batch_df.select(cols_to_write)

        batch_df_to_write.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://localhost:5432/sqlpad") \
            .option("dbtable", "projet_bdcc") \
            .option("user", "sqlpad") \
            .option("password", "sqlpad") \
            .option("driver", "org.postgresql.Driver") \
            .save()

        logging.info(f"Batch {batch_id} écrit dans PostgreSQL avec {batch_df_to_write.count()} lignes.")
    except Exception as e:
        logging.error(f"Erreur écriture batch {batch_id} : {e}")


# Stream vers PostgreSQL
query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .option("checkpointLocation", "/tmp/spark_checkpoints_postgres") \
    .start()


# Stream vers console pour debug
console_query = json_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .trigger(processingTime="1 second") \
    .start()


# Attente de la terminaison
query.awaitTermination()
console_query.awaitTermination()
