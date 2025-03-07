from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, from_unixtime, expr, hour, to_date

''' Spark Session Initiliazation '''
spark = SparkSession.builder \
    .appName("BikeSharingAnalytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BOOTSTRAP_SERVERS = "172.25.0.13:9092" # Kafka servers
STATION_STATUS_TOPIC = "station_status"      # topic 1
WEATHER_TOPIC = "weather_info"               # topic 2
STATION_INFO_TOPIC = "station_information"   # topic 3

station_status_schema = StructType([ # Schema for station status topic 
    StructField("station_id", StringType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("num_ebikes_available", IntegerType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_bikes_disabled", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("num_docks_disabled", IntegerType(), True),
    StructField("eightd_has_available_keys", BooleanType(), True),
    StructField("last_reported", LongType(), True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_returning", IntegerType(), True)
])

weather_schema = StructType([  # Schema for weather info
    StructField("location_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("cloudiness", IntegerType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("temperature", FloatType(), True)
])

station_info_schema = StructType([  # Schema for station info
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("capacity", IntegerType(), True)
])


''' Initialize reading of topics '''
station_status_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", STATION_STATUS_TOPIC) \
    .load()

station_status_parsed = station_status_df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), station_status_schema).alias("parsed")
).select(
    col("parsed.*"),
    from_unixtime(col("parsed.last_reported")).alias("last_updated")
).withColumn(
    "status_hour", hour(col("last_updated"))
).withColumn(
    "status_date", to_date(col("last_updated"))
)

weather_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", WEATHER_TOPIC) \
    .load()

weather_parsed = weather_df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), weather_schema).alias("parsed")
).select(
    col("parsed.*")
).withColumn(
    "weather_hour", hour(col("timestamp"))
).withColumn(
    "weather_date", to_date(col("timestamp"))
)

station_info_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", STATION_INFO_TOPIC) \
    .load()

station_info_parsed = station_info_df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), station_info_schema).alias("parsed")
).select("parsed.*")


station_status_parsed.createOrReplaceTempView("station_status")  # view of station status
weather_parsed.createOrReplaceTempView("weather_data")           # view of weather info
station_info_parsed.createOrReplaceTempView("station_info")      # view of station info

print('Station status parsed')
station_status_parsed.show(truncate=False)
print('Station info parsed')
station_info_parsed.show(truncate=False)
print('weather info parsed')
weather_parsed.show(truncate=False)


''' 2.3 '''
''' Join station information with status data '''
joined_query = """
SELECT distinct
    ss.station_id,
    si.name AS station_name,
    si.lat AS latitude,
    si.lon AS longitude,
    si.capacity AS capacity,
    ss.num_bikes_available AS available_bikes,
    ss.num_docks_available AS docks_available,
    (ss.num_bikes_available / (ss.num_bikes_available + ss.num_docks_available)) AS utilization_rate,
    ss.last_updated,
    wd.temperature,
    wd.wind_speed,
    wd.cloudiness
FROM 
    station_status ss
INNER JOIN 
    station_info si 
ON 
    ss.station_id = si.station_id
INNER JOIN 
    weather_data wd
ON 
    ss.status_date = wd.weather_date
"""

joined_df = spark.sql(joined_query)


''' Utilization rate per station'''
system_utilization_query = """
SELECT 
    AVG(utilization_rate) AS avg_utilization_rate,
    MAX(utilization_rate) AS max_utilization_rate,
    MIN(utilization_rate) AS min_utilization_rate,
    AVG(temperature) AS avg_temperature,
    AVG(wind_speed) AS avg_wind_speed,
    AVG(cloudiness) AS avg_cloudiness
FROM 
    (
        SELECT 
            station_id,
            utilization_rate,
            temperature,
            wind_speed,
            cloudiness,
            precipitation
        FROM 
            (
                SELECT 
                    ss.station_id,
                    (ss.num_bikes_available / (ss.num_bikes_available + ss.num_docks_available)) AS utilization_rate,
                    wd.temperature,
                    wd.wind_speed,
                    wd.cloudiness,
                    wd.precipitation
                FROM 
                    station_status ss
                INNER JOIN 
                    weather_data wd
                ON 
                    ss.status_date = wd.weather_date
            ) AS joined_data
    )
"""

system_utilization_df = spark.sql(system_utilization_query)

''' Usage summary per hour and day'''
hourly_usage_summary_query = """
SELECT 
    status_hour, status_date,
    COUNT(DISTINCT station_id) AS active_stations,
    SUM(num_bikes_available) AS total_bikes_available,
    SUM(num_docks_available) AS total_docks_available,
    AVG(temperature) AS avg_temperature,
    AVG(wind_speed) AS avg_wind_speed
FROM 
    station_status ss
INNER JOIN 
    weather_data wd
ON 
    ss.status_date = wd.weather_date
GROUP BY 
    status_hour, status_date
ORDER BY 
    status_date, status_hour
"""

hourly_usage_summary_df = spark.sql(hourly_usage_summary_query)

print("Joined Data:")
joined_df.show(truncate=False)
print("Overall System Utilization:")
system_utilization_df.show(truncate=False)
print("Hourly Usage Summaries:")
hourly_usage_summary_df.show(truncate=False)



''' Save to SQLite and CSV '''
import sqlite3
import pandas as pd
import os

def save_to_sqlite(df, table_name):
    temp_dir = "/app/temp_output" 
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
    full_file_path = os.path.join(temp_dir, part_file)
    df_pandas = pd.read_csv(full_file_path)
    
    conn = sqlite3.connect('/app/bike_sharing.db')
    df_pandas.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print("Data saved to SQLite.")

save_to_sqlite(joined_df, "joined_data")
save_to_sqlite(system_utilization_df, "system_utilization")
save_to_sqlite(hourly_usage_summary_df, "hourly_usage_summary")

