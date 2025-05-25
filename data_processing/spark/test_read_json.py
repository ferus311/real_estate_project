#!/usr/bin/env python3
"""
Test đọc JSON file từ HDFS để debug vấn đề
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def test_read_json():
    # Tạo Spark session đơn giản
    spark = SparkSession.builder \
        .appName("Test Read JSON") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    print("✅ Spark session created")
    
    # Đường dẫn test
    test_path = "/data/realestate/raw/batdongsan/house/2025/05/23"
    
    try:
        print(f"🔍 Testing path: {test_path}")
        
        # Đọc JSON đơn giản không schema
        print("📖 Reading JSON without schema...")
        df = spark.read.json(test_path)
        
        print(f"🎯 Schema inferred:")
        df.printSchema()
        
        print(f"📊 Row count: {df.count()}")
        
        print("📋 Sample data:")
        df.show(5, truncate=False)
        
        print("✅ Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    test_read_json()
