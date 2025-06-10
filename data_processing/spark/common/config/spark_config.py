"""
Cấu hình Spark cho các job xử lý
Chỉ chứa các hàm thực sự được sử dụng trong codebase
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name, master="spark://spark-master:7077", config=None):
    """
    Tạo và cấu hình SparkSession cho ETL processing

    Args:
        app_name (str): Tên ứng dụng Spark
        master (str, optional): Master URL. Mặc định là "spark://spark-master:7077".
        config (dict, optional): Cấu hình bổ sung. Mặc định là None.

    Returns:
        SparkSession: SparkSession đã được cấu hình
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    # Cấu hình cơ bản tối ưu cho real estate data processing
    builder = (
        builder.config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.shuffle.partitions", "50")  # Tối ưu cho data size
        .config("spark.speculation", "true")
        .config("spark.sql.adaptive.enabled", "true")  # Adaptive Query Execution
        # PostgreSQL JDBC Driver
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.0.jar")
        # Cấu hình cho HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )
    return builder.getOrCreate()


def create_optimized_ml_spark_session(
    app_name, master="spark://spark-master:7077", config=None
):
    """
    Tạo SparkSession được tối ưu đơn giản cho ML workloads

    Tính năng tối ưu:
    - Memory management cho ML
    - Adaptive query execution
    - Arrow optimization cho Pandas conversion

    Args:
        app_name (str): Tên ứng dụng Spark
        master (str, optional): Master URL. Mặc định là "spark://spark-master:7077".
        config (dict, optional): Cấu hình bổ sung. Mặc định là None.

    Returns:
        SparkSession: SparkSession đã được tối ưu cho ML
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    # Cấu hình tối ưu đơn giản cho ML workloads
    ml_optimized_config = {
        # Memory Management - Increased for better performance
        # "spark.driver.memory": "4g",
        "spark.executor.memory": "6g",
        # "spark.executor.cores": "3",
        # "spark.driver.maxResultSize": "3g",
        # Adaptive Query Execution (AQE)
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # Arrow Optimization for Pandas Conversion
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        # "spark.sql.execution.arrow.maxRecordsPerBatch": "50000",
        # Shuffle Optimization
        "spark.sql.shuffle.partitions": "50",
        # Python Optimization
        "spark.python.worker.reuse": "true",
        # PostgreSQL JDBC Driver
        "spark.jars": "/opt/bitnami/spark/jars/postgresql-42.7.0.jar",
        # HDFS Configuration
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    }

    # Apply ML-optimized configuration
    for key, value in ml_optimized_config.items():
        builder = builder.config(key, value)

    # Thêm cấu hình tùy chỉnh
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    print("⚡ Simplified ML-optimized SparkSession created")
    return spark
