#!/bin/bash

KAFKA_HOME="/mnt/e/Library/kafka_2.12-2.3.0"
TOPIC_NAME="twitter"

$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $TOPIC_NAME