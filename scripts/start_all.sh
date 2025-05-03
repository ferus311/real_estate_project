#!/bin/bash
docker compose -f ./docker/airflow.yml -f ./docker/spark.yml -f ./docker/hdfs.yml -f ./docker/crawler.yml -f ./docker/kafka.yml up -d
