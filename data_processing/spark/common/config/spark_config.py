"""
Cấu hình Spark cho các job xử lý
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name, master="spark://spark-master:7077", config=None):
    """
    Tạo và cấu hình SparkSession

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
        .config(
            "spark.sql.shuffle.partitions", "50"
        )  # Giảm từ 100 để phù hợp data size
        .config("spark.speculation", "true")
        .config("spark.sql.adaptive.enabled", "true")  # Adaptive Query Execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Cấu hình cho HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        # Cấu hình memory cho large datasets
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    )

    # Cấu hình cho Delta Lake - Tạm thời vô hiệu hóa để tránh lỗi khi không có thư viện
    # builder = (
    #     builder.config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    # )

    # Thêm cấu hình tùy chỉnh
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def get_default_spark_config():
    """
    Trả về cấu hình Spark mặc định

    Returns:
        dict: Cấu hình Spark mặc định
    """
    return {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",
        "spark.sql.shuffle.partitions": "100",
        "spark.speculation": "true",
        # Tạm thời vô hiệu hóa Delta Lake để tránh lỗi
        # "spark.jars.packages": "io.delta:delta-core_2.12:2.1.0",
        # "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        # "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
    }


def optimize_spark_for_local(spark):
    """
    Tối ưu cấu hình Spark cho chạy local

    Args:
        spark (SparkSession): SparkSession

    Returns:
        SparkSession: SparkSession đã được tối ưu
    """
    spark.conf.set("spark.driver.memory", "2g")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    return spark
