from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Preview HDFS Data") \
    .getOrCreate()

# Đường dẫn file HTML crawl lưu trên HDFS
input_path = "hdfs://namenode:9000/raw/example.html"

try:
    # Đọc file HTML dưới dạng văn bản
    rdd = spark.sparkContext.textFile(input_path)
    print("📄 Nội dung file HTML:")
    for line in rdd.take(20):  # Hiển thị 20 dòng đầu tiên
        print(line)

except Exception as e:
    print(f"❌ Lỗi khi đọc dữ liệu từ HDFS: {e}")
finally:
    spark.stop()
