# 1st-spark-kafka-cloud-project

### Introduction 
This is my first project. The goal is to build a reliable, scalable and secure pipeline and push it to the cloud. The extra will be automation with airflow and dockerisation for compatibility. There won't be any transformation made on the data, since I already know how spark.sql works. I will update this README as I am going forward with the project. Every change made won't be commited to the main branch so we will be able to compare the initial project from the final product.

In the future, I might decide to merge this data with data from another project as part of a bigger project.

### 25/06/2023 - Start 

Creation of the producer.py and spark_consumer.py files. The pipelines works well using a local kafka broker to pub/sub messages. 

The next step are
- Schema Registry and Serialization
-  Implement Data Quality Checks
-  Efficiency / Compression
-  Idempotency
-  Security

I plan to achieve all of that by June 30. Then I'll tackle how to get the pipeline to work in the cloud.

### 25/06/2023 - Update

I could not serialize my data to AVRO in order to use schema registry. I am currently using windows and windows does not support Confluent Schema Registry yet. I have to set up a docker container to be able to use Schema Registry. This will be another extra step. For now I focus on sedding the final data to a s3 bucket for it to be accessible. I know it would be simpler just to fetch the data from APIs and directly store it in s3, but I want to learn how all of this work so I am making it complicated.

Here are the next steps:
- Sending data to s3
- Implement Data Quality Checks
- Efficiency / Compression
- Idempotency
- Security
  
