#!/bin/bash
docker compose -f ./docker/airflow.yml -f ./docker/spark.yml -f ./docker/hdfs.yml up -d
