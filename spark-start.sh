#!/bin/bash

SPARK_HOME="/mnt/e/Library/spark-2.4.3-bin-hadoop2.7"

# run the spark master server
$SPARK_HOME/sbin/start-master.sh -i 127.0.0.1 -p 7077 --webui-port 7078