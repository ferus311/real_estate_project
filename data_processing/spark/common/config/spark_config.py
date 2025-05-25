"""
Cấu hình Spark đơn giản
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name, master="spark://spark-master:7077"):
    """
    Tạo SparkSession với cấu hình tối ưu cho data processing
    """

    try:
        # Thử connect cluster trước với optimized config
        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.driver.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
            # Timeout and performance settings
            .config("spark.network.timeout", "800s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.sql.broadcastTimeout", "36000")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # JSON reading optimizations
            .config(
                "spark.sql.files.maxPartitionBytes", "134217728"
            )  # 128MB per partition
            .config("spark.sql.files.openCostInBytes", "4194304")  # 4MB open cost
            .getOrCreate()
        )
        print(f"✅ Spark cluster connected: {app_name}")
        return spark

    except Exception as e:
        print(f"❌ Cluster failed: {e}")
        print("🔄 Using local mode with optimized config...")

        # Fallback local với same optimizations
        spark = (
            SparkSession.builder.appName(f"{app_name}_local")
            .master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # Same timeout settings for consistency
            .config("spark.network.timeout", "800s")
            .config("spark.sql.broadcastTimeout", "36000")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # JSON reading optimizations
            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.sql.files.openCostInBytes", "4194304")
            .getOrCreate()
        )
        print("✅ Local Spark created with optimizations")
        return spark
