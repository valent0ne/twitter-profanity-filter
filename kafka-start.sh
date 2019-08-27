#!/bin/bash

KAFKA_HOME="/mnt/e/Library/kafka_2.12-2.3.0"

# cleaning
rm -rf /tmp/kafka-logs/

# run the kafka server
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

# create the topic
# bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic twitter