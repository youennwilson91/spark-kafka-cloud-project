from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
        .appName("meteo_pipeline") \
        .master("spark://35.180.124.171:7077") \
        .config("spark.executor.instances", "2") \
        .getOrCreate()

read = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "b-1.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092, b-2.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092")\
    .option("subscribe", "weather")\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()

schema = StructType()\
    .add("city", StringType())\
    .add("country", StringType())\
    .add("time", StringType()) \
    .add("weather", StringType())\
    .add("description", StringType())\
    .add("temp", StringType()) \
    .add("air_quality", StringType()) \
    .add("5_days_forecast", StringType())

base_1 = read.selectExpr("CAST(value as STRING) as value") #kafka stores data into 4 column "key", "value", "timestamp" and "". We want to get the "value" column and cast it as a string
base_2 = base_1.select(from_json(col("value"), schema).alias("data")) #then we json stringto a dataframe by applying our schema"
base_3 = base_2.select("data.*") #We can then select everything from the dataframe


query_2 = base_3.writeStream\
    .outputMode("append")\
    .format("csv")\
    .option("path", "s3a://sewa-meteo-bucket-1/files/")\
    .option("checkpointLocation", "s3a://sewa-meteo-bucket-1/checkpoint")\
    .start()

query_2.awaitTermination()


#.option("path", "https://sewa-meteo-bucket-1.s3.eu-west-3.amazonaws.com/csv/weather.csv")\
#.option("checkpointLocation", "C:/Users/sewa/Desktop/csv/checkpoint")\

