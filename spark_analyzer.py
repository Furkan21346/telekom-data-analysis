import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    spark_version = pyspark.__version__
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"

    print(f"Spark Version Of The System: {spark_version}")
    print(f"Used Kafka Package: {kafka_package}\n")

    spark = SparkSession.builder \
        .appName("TelecomTrafficAnalyzer") \
        .config("spark.jars.packages", kafka_package) \
        .getOrCreate()

    # Reduce log verbosity so we can see our data clearly
    spark.sparkContext.setLogLevel("WARN")

    # 2. Define Data Schema
    # This must match exactly with the JSON created by data_generator.py
    telecom_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("base_station", StringType(), True),
        StructField("usage_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("Spark Streaming started. Waiting for Kafka messages...\n")

    # 3. Read Stream from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "telecom_traffic") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parse Binary Kafka Data to JSON
    # Kafka sends 'value' as binary (byte). First cast to String, then parse to JSON schema.
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), telecom_schema).alias("data")) \
        .select("data.*")

    # 5. Real-time Aggregation: Count errors per base station
    # Filter only failing/dropped connections and group them
    df_errors = df_parsed.filter(col("status").isin("Error", "Connection_Dropped")) \
        .groupBy("base_station", "status") \
        .count()

    # 6. Output to Console
    # outputMode("update") only prints the rows that have changed (updated counts)
    query = df_errors.writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    # Keep the streaming process running indefinitely
    query.awaitTermination()

if __name__ == "__main__":
    main()