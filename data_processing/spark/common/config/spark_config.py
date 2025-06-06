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
        # Cấu hình cho HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    # Thêm cấu hình tùy chỉnh
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def create_optimized_ml_spark_session(
    app_name, master="spark://spark-master:7077", config=None
):
    """
    Tạo SparkSession được tối ưu đặc biệt cho ML workloads

    Tính năng tối ưu:
    - Memory management cho ML
    - Adaptive query execution
    - Broadcast optimization
    - Arrow optimization cho Pandas conversion

    Args:
        app_name (str): Tên ứng dụng Spark
        master (str, optional): Master URL. Mặc định là "spark://spark-master:7077".
        config (dict, optional): Cấu hình bổ sung. Mặc định là None.

    Returns:
        SparkSession: SparkSession đã được tối ưu cho ML
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    # Cấu hình tối ưu cho ML workloads
    ml_optimized_config = {
        # Memory Management
        "spark.driver.memory": "3g",
        "spark.executor.memory": "6g",
        "spark.executor.cores": "3",
        "spark.driver.maxResultSize": "2g",
        # Adaptive Query Execution (AQE)
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        # Broadcast Optimization
        "spark.sql.broadcastTimeout": "300",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "50MB",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "50MB",
        # Arrow Optimization for Pandas Conversion
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
        # Shuffle and Partition Optimization
        "spark.sql.shuffle.partitions": "100",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
        # Checkpointing
        "spark.sql.streaming.checkpointLocation": "/tmp/spark-ml-checkpoint",
        # Python Optimization
        "spark.python.worker.memory": "2g",
        "spark.python.worker.reuse": "true",
        # HDFS Configuration
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        # Performance Monitoring
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/tmp/spark-events",
        # Additional ML-specific optimizations
        "spark.sql.execution.arrow.fallback.enabled": "false",
        "spark.sql.adaptive.join.enabled": "true",
    }

    # Apply ML-optimized configuration
    for key, value in ml_optimized_config.items():
        builder = builder.config(key, value)

    # Thêm cấu hình tùy chỉnh
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir("/tmp/spark-ml-checkpoints")

    print("⚡ ML-optimized SparkSession created with broadcast optimization")
    return spark
