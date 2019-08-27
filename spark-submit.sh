#!/bin/bash

SPARK_HOME="/mnt/e/Library/spark-2.4.3-bin-hadoop2.7"
JAR_PATH="/mnt/e/Projects/big-data-project/twitter-spark-consumer/target/twitterSparkConsumer-1.0-SNAPSHOT.jar"

# run the spark master server
$SPARK_HOME/bin/spark-submit \
            --class it.univaq.bigdata.twitterSparkConsumer.Main \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 \
            --master local[*] \
            $JAR_PATH