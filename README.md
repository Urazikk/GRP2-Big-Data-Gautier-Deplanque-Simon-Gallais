# GRP2 - Big Data Processing - ECE Fall 2026

Lab work for the Big Data Processing course.

## Group

- Group: gr-02 (GRP2)
- Simon Gallais - git username: `Urazikk` - simon.gallais@edu.ece.fr
- Gautier Deplanque - git username: `gautierdpl` - gautier.deplanque@edu.ece.fr

## Contents

| Lab | Topic | Folder |
|-----|-------|--------|
| Lab 1 | Unstructured data analysis with Spark RDDs (word count) | [`lab01-word-count`](lab01-word-count) |
| Lab 2 | Structured data analysis with DataFrames and Spark SQL (NYC taxi, Jan 2019 vs Jan 2026) | [`lab02-sparksql-dataframes`](lab02-sparksql-dataframes) |
| Lab 3 | Kafka: producer / topic / consumer, a book streamed line by line and cleaned | [`lab03-kafka`](lab03-kafka) |

Each lab folder has its own README (what is done, main results, how to run it). Labs 1 and 2 have an
executed notebook that can be read directly on GitHub, Lab 3 is plain Python scripts.

## Environment

The labs run in the Jupyter Docker Stacks image `quay.io/jupyter/pyspark-notebook`
(Spark + Python + Jupyter). Lab 3 uses the `apache/kafka-native` image and a local Python
environment. See the README of each lab for the exact command.
