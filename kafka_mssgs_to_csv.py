from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("CombineKafkaMessages") \
    .getOrCreate()

BOOTSTRAP_SERVERS = "172.25.0.13:9092"
STATION_STATUS_TOPIC = "station_status"
WEATHER_TOPIC = "weather_data"
STATION_INFO_TOPIC = "station_information"

station_status_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", STATION_STATUS_TOPIC) \
    .load() \
    .selectExpr("CAST(value AS STRING) as message") \
    .withColumn("source", lit("station_status"))

station_info_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", STATION_INFO_TOPIC) \
    .load() \
    .selectExpr("CAST(value AS STRING) as message") \
    .withColumn("source", lit("station_information"))

weather_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", WEATHER_TOPIC) \
    .load() \
    .selectExpr("CAST(value AS STRING) as message") \
    .withColumn("source", lit("weather_data"))

combined_df = station_status_raw.union(station_info_raw).union(weather_raw)
output_path = "/app/output/combined_kafka_messages.csv"
combined_df.write.mode("overwrite").option("header", "true").csv(output_path)

print("Combined Kafka messages saved to: {}".format(output_path))
