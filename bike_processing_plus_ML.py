from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, from_unixtime, hour, to_date, dayofweek, row_number, avg, max, min, stddev, lead
from pyspark.sql import Window

''' Spark Session Initiliazation '''
spark = SparkSession.builder \
    .appName("BikeSharingAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("### Spark Session Initialized ###")


BOOTSTRAP_SERVERS = "172.25.0.13:9092"  # Kafka servers
STATION_STATUS_TOPIC = "station_status"       # topic 1
WEATHER_TOPIC = "weather_info"                # topic 2
STATION_INFO_TOPIC = "station_information"    # topic 3

station_status_schema = StructType([    # Schema for station status topic 
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

weather_schema = StructType([         # Schema for weather info
    StructField("location_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("cloudiness", IntegerType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("temperature", FloatType(), True)
])


station_info_schema = StructType([     # Schema for station info
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

window_spec = Window.partitionBy("station_id", "status_date").orderBy(col("last_reported").desc())
station_status_parsed = station_status_parsed.withColumn(
    "row_num", row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

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

weather_parsed = weather_parsed.groupBy("weather_date").agg(
    avg("temperature").alias("avg_temperature"),
    avg("wind_speed").alias("avg_wind_speed"),
    avg("cloudiness").alias("avg_cloudiness"),
    max("precipitation").alias("max_precipitation")
)

station_info_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", STATION_INFO_TOPIC) \
    .load()

station_info_parsed = station_info_df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), station_info_schema).alias("parsed")
).select("parsed.*")

station_status_parsed.createOrReplaceTempView("station_status") # view of station status
weather_parsed.createOrReplaceTempView("weather_data")          # view of weather data
station_info_parsed.createOrReplaceTempView("station_info")     # view of station info

''' Join station information with status data '''
joined_query = """
SELECT
    ss.station_id,
    si.name AS station_name,
    si.lat AS latitude,
    si.lon AS longitude,
    si.capacity AS capacity,
    ss.num_bikes_available AS available_bikes,
    ss.num_docks_available AS docks_available,
    (ss.num_bikes_available / (ss.num_bikes_available + ss.num_docks_available)) AS utilization_rate,
    ss.last_updated,
    ss.status_date,
    wd.avg_temperature AS temperature,
    wd.avg_wind_speed AS wind_speed,
    wd.avg_cloudiness AS cloudiness,
    HOUR(ss.last_updated) AS hour_of_day
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

''' Stats trends for utilization '''
utilization_summary = joined_df.groupBy("status_date").agg(
    avg("utilization_rate").alias("average_docking_station_utilisation"),
    max("utilization_rate").alias("max_docking_station_utilisation"),
    min("utilization_rate").alias("min_docking_station_utilisation"),
    stddev("utilization_rate").alias("std_dev_docking_station_utilisation")
)

print("Utilization Summary Schema:")
utilization_summary.printSchema()
print("Sample Utilization Summary:")
utilization_summary.show(5, truncate=False)

''' next hour features '''
joined_features = joined_df.withColumn("day_of_week", dayofweek(col("last_updated")))
window_spec = Window.partitionBy("station_id").orderBy("last_updated")
joined_features = joined_features.withColumn(
    "next_hour_utilization", lead("utilization_rate", 1).over(window_spec)
).withColumn(
    "forecasted_temperature", lead("temperature", 1).over(window_spec)
).withColumn(
    "forecasted_wind_speed", lead("wind_speed", 1).over(window_spec)
).withColumn(
    "forecasted_cloudiness", lead("cloudiness", 1).over(window_spec)
).filter(col("next_hour_utilization").isNotNull())


ml_features = joined_features.select(
    col("forecasted_temperature").alias("temperature"),
    col("forecasted_wind_speed").alias("wind_speed"),
    col("forecasted_cloudiness").alias("cloudiness"),
    col("hour_of_day"),
    col("capacity"),
    col("next_hour_utilization").alias("utilization_rate"),
    col("station_id"),
    col("station_name").alias("name")
)

''' 80% data for training - 20% for testing '''
''' Prediction for the next hour '''
train_data, test_data = ml_features.randomSplit([0.8, 0.2], seed=42)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

feature_cols = ["temperature", "wind_speed", "cloudiness", "hour_of_day", "capacity"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
rf_model = RandomForestRegressor(featuresCol="features", labelCol="utilization_rate")

pipeline = Pipeline(stages=[vector_assembler, rf_model])
model = pipeline.fit(train_data)
predictions = model.transform(test_data)
predictions_with_metadata = predictions.select(
    "features", "utilization_rate", "prediction", "station_id", "name"
)

print("Predictions with Metadata Schema:")
predictions_with_metadata.printSchema()
print("Sample Predictions with Metadata:")
predictions_with_metadata.sample(fraction=0.1).distinct().limit(20).show(truncate=False)

''' Model evaluation '''
evaluator_rmse = RegressionEvaluator(
    labelCol="utilization_rate", predictionCol="prediction", metricName="rmse"
)
rmse = evaluator_rmse.evaluate(predictions)
print("Root Mean Squared Error (RMSE):", rmse)

''' Model feature importance plot '''
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
features = ["Temperature", "Wind Speed", "Cloudiness", "Hour of Day", "Capacity"]
feature_importances = model.stages[-1].featureImportances.toArray()

plt.figure(figsize=(10, 6))
plt.bar(features, feature_importances, color='orange', alpha=0.7)
plt.title('Feature Contributions to the Model')
plt.xlabel('Features')
plt.ylabel('Feature Importance')
plt.grid(axis='y')



output_file = 'model_rmse_feature_importance.png'
plt.savefig(output_file, bbox_inches='tight')
plt.close()

print("Feature importance plot saved to:", output_file)


from pyspark.sql.functions import abs, col, mean

''' Mean Absolute Error '''
predictions_with_mae = predictions.withColumn(
    "absolute_error",
    abs(col("utilization_rate") - col("prediction"))
)
mae = predictions_with_mae.select(mean("absolute_error")).first()[0]
print("Mean Absolute Error (MAE):", mae)

''' model save'''
#model_path = "model"
#model.save(model_path)


import json
import pandas as pd
import os

metadata = {
    "rmse": rmse,
    "mae": mae,
    "number_of_features": len(feature_cols),
    "feature_importances": dict(zip(features, feature_importances.tolist())),
    "data_points": {
        "training_data_count": train_data.count(),
        "testing_data_count": test_data.count()
    }
}
metadata_file = "run_metadata.json"
with open(metadata_file, "w") as f:
    json.dump(metadata, f, indent=4)

print("Metadata saved...")

''' Check if the file exists and delete it if it does'''
predictions_csv = "predictions_with_metadata.csv"
if os.path.exists(predictions_csv):
    os.remove(predictions_csv)
    print("Predictions file already exists. Deleting...")

''' Predictions to CSV '''
predictions_with_metadata_df = predictions_with_metadata.toPandas()
predictions_csv = "predictions_with_metadata.csv"
predictions_with_metadata_df.to_csv(predictions_csv, index=False)
print("Predictions saved...")

''' Feature importance to CSV '''
feature_importances_df = pd.DataFrame({
    "Feature": features,
    "Importance": feature_importances
})
feature_importances_csv = "feature_importances.csv"
feature_importances_df.to_csv(feature_importances_csv, index=False)
print("Feature importances saved!")