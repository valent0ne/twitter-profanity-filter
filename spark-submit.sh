#!/bin/bash

SPARK_HOME="/mnt/e/Library/spark-2.4.3-bin-hadoop2.7"
JAR_PATH="/mnt/e/Projects/twitter-profanity-filter/twitter-spark-consumer/target/twitter-spark-consumer-1.0.jar"

# run the spark master server
$SPARK_HOME/bin/spark-submit \
            --class it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.Main \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 \
            --master local[*] \
            $JAR_PATH