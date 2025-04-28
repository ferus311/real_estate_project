from pyspark.sql import SparkSession

# HDFS config
HDFS_URL = "hdfs://namenode:9870"
HDFS_DIR = "/data/test/raw_data"

def main():
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("View HDFS Data") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .config("spark.rpc.message.maxSize", "128") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    # Đọc dữ liệu từ HDFS (Parquet format)
    try:
        print(f"Reading data from HDFS directory: {HDFS_DIR}")
        df = spark.read.parquet(HDFS_DIR)

        # Hiển thị schema của dữ liệu
        print("Schema of the data:")
        df.printSchema()

        # Hiển thị 10 dòng đầu tiên
        print("First 10 rows of the data:")
        df.show(10, truncate=False)

        # Đếm số lượng bản ghi
        print(f"Total number of records: {df.count()}")

    except Exception as e:
        print(f"Error reading data from HDFS: {e}")

    finally:
        # Dừng SparkSession
        spark.stop()

if __name__ == "__main__":
    main()
