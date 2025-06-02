#!/bin/bash

set -e

echo "🛠️ Tạo các thư mục cần thiết..."

# HDFS
mkdir -p docker/volumes/hdfs/namenode
mkdir -p docker/volumes/hdfs/datanode1
mkdir -p docker/volumes/hdfs/datanode2
mkdir -p docker/volumes/hdfs/datanode3

# Kafka & ZooKeeper
mkdir -p docker/volumes/kafka
mkdir -p docker/volumes/zookeeper

# Airflow
mkdir -p data_processing/airflow/dags
mkdir -p data_processing/airflow/logs
mkdir -p data_processing/airflow/plugins

# Spark
mkdir -p data_processing/spark/jobs

# Notebooks
mkdir -p data_processing/notebooks

echo "✅ Đã tạo tất cả thư mục."

echo "🔧 Phân quyền cho Kafka & Zookeeper (UID:GID = 1000:1000)..."
sudo chown -R 1000:1000 docker/volumes/kafka
sudo chown -R 1000:1000 docker/volumes/zookeeper
chmod -R 755 docker/volumes/kafka
chmod -R 755 docker/volumes/zookeeper

echo "🎉 Hoàn tất. Tất cả thư mục và phân quyền đã sẵn sàng!"
