### Import
```
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import os
```

We firstly initialize a Spark Session:
```
spark = SparkSession.builder \
        .appName("meteo_pipeline") \
        .master("spark://[your_master_node_ip]:7077") \
        .config("spark.executor.instances", "2") \ #Since we are running the producer and the consumer on the same EC2 machine, we have to monitor the ressources allowed for each. You may delete this line if your machine is only consuming from kafka.
        .getOrCreate()
```

We then read the upcoming stream from the kafka topic that stores cities climate information

```
read = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "[broker1-ip], [broker2-ip]")\ #we setted up two brokers in the AWS MSK cluster.
    .option("subscribe", "weather")\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()
```

Since the data that comes from kafka is under a json utf-8 encoded form, we have to turn it back into a dataframe. We first define a schema, that corresponds to the new_info dictionary of the prodeucer.py file. We will then be able to retrieve the original dataframe.

```
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
```

We finally write the data to a S3 bucket.

```
query_2 = base_3.writeStream\
    .outputMode("append")\
    .format("csv")\
    .option("path", "your_bucket_folder_path")\
    .option("checkpointLocation", ""your_bucket_checkpoint_folder_path")\
    .start()

query_2.awaitTermination()
```

Simple !
