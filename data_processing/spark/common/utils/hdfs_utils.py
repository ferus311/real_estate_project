"""
Tiện ích làm việc với HDFS
"""

import os
from pyspark.sql import SparkSession


def check_hdfs_path_exists(spark, hdfs_path):
    """
    Kiểm tra đường dẫn HDFS có tồn tại không

    Args:
        spark (SparkSession): SparkSession
        hdfs_path (str): Đường dẫn HDFS

    Returns:
        bool: True nếu đường dẫn tồn tại, False nếu không
    """
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    return fs.exists(path)


def list_hdfs_files(spark, hdfs_path):
    """
    Liệt kê các file trong thư mục HDFS

    Args:
        spark (SparkSession): SparkSession
        hdfs_path (str): Đường dẫn HDFS

    Returns:
        list: Danh sách các file
    """
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)

    if not fs.exists(path):
        return []

    files = []
    file_statuses = fs.listStatus(path)
    for file_status in file_statuses:
        files.append(file_status.getPath().toString())

    return files


def ensure_hdfs_path(spark, hdfs_path):
    """
    Đảm bảo đường dẫn HDFS tồn tại (tạo nếu không có)

    Args:
        spark (SparkSession): SparkSession
        hdfs_path (str): Đường dẫn HDFS
    """
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)

    if not fs.exists(path):
        fs.mkdirs(path)
        print(f"Đã tạo đường dẫn HDFS: {hdfs_path}")
