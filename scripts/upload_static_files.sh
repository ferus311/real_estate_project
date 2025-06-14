#!/bin/bash

# Upload static files to HDFS for ML training
# This script uploads CSV files and other static data needed by ML pipelines

echo "🚀 Uploading static files to HDFS..."

# Wait for HDFS to be ready
echo "⏳ Waiting for HDFS to be ready..."
sleep 10

# Create HDFS directories
echo "📁 Creating HDFS directories..."
docker exec -it namenode hdfs dfs -mkdir -p /data/realestate/static

# Upload province population density CSV
echo "📊 Uploading province population density CSV..."
docker cp data_processing/ml/utils/province_stats/province_population_density.csv namenode:/tmp/
docker exec -it namenode hdfs dfs -put /tmp/province_population_density.csv /data/realestate/static/

# Verify upload
echo "✅ Verifying uploads..."
docker exec -it namenode hdfs dfs -ls /data/realestate/static/

echo "🎉 Static files upload completed!"
