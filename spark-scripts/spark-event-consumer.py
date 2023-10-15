import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import window, col, from_json, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# (
#     stream_df.selectExpr("CAST(value AS STRING)")
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )


# Define the schema for your JSON data
json_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("furniture", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("ts", TimestampType(), True)
])

# Deserialize the JSON data
parsed_stream = stream_df.selectExpr("CAST(value AS STRING)").select(
    from_json("value", json_schema).alias("data")
)

# Show the content of parsed_stream
# parsed_stream.show()
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Apply your aggregation
aggregated_data = parsed_stream.groupBy('data.furniture').agg(count("*").alias("total_purchase"))

# Write the result to the console
aggregated_data.writeStream.outputMode("complete").format("console").start().awaitTermination()


