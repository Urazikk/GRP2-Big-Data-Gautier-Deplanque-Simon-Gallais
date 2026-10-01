#!/bin/bash
# Start a single Kafka broker (KRaft mode, no Zookeeper) on localhost:9092
docker pull apache/kafka-native:4.1.1
docker run -d --name kafka -p 9092:9092 apache/kafka-native:4.1.1
