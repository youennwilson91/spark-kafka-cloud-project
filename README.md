# 1st-spark-kafka-cloud-pipeline

### Introduction 
This is my first project. The goal is to build a pipeline that fetches climate information on european cities, send it to kafka and retrieve it before storing it. The pipeline will be pushed to the cloud using an EC2 machine that runs spark and act both as a message producer and consumer. Kafka will be hosted on a MSK cluster. Data will be stored on s3. This pipeline was build for practice purposes only and there won't be any transformation made on the data since I already know how spark.sql works.

The next pipeline I will build will modify this one by introducing AWS Glue. I will store the data into AVRO or Parquet format since it is more suitable for complex data types, schema evolution and splittability. I will also use AWS Kinesis to buffer the data in order to handle large data workloads.

### Setting up the environment
For information on how to set up an MSK cluster and connect it to an EC2 instance, check the following video: https://www.youtube.com/watch?v=5WaIgJwYpS8
Make sure that the role associated with your EC2 instance allows MSK full acess and S3 full access so you won't bump into authorization errors.

### Setting up Spark on the EC2 machine.
To be able to run spark jobs on the EC2 machine, you will have to install Spark and Java. You will find bellow the commands that you have to run in order for your EC2 instance to run spark jobs.

```
sudo apt-get update
sudo apt-get install openjdk-11-jdk

sudo nano ~/.bashrc

#add the 4 following lines at the end of the file

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PATH=$PATH:$JAVA_HOME/bin

#close and save the file 
source ~/.bashrc

start-master.sh 
start-worker.sh spark://<master-node-ip>:7077
```

When running the spark job using the command spark-submit, you will have to specify two packages: hadoop3 and kafka. You will find bellow an exemple to run the producer.py script

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.0 producer.py
```

Check the .md files for a walkthrough of the producer and spark_consumer scripts.

Don't hesitate to give feedback. I am trying to learn as much as I can !

Thank you.


