from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("meteo_pipeline").getOrCreate()

read = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "european_cities")\
    .option("startingOffsets", "latest")\
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

query = base_3.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()

query.awaitTermination()
