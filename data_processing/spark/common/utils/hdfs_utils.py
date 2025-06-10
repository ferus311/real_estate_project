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


def create_hdfs_directory(spark, hdfs_path):
    """
    Tạo thư mục HDFS và trả về kết quả thành công

    Args:
        spark (SparkSession): SparkSession
        hdfs_path (str): Đường dẫn HDFS cần tạo

    Returns:
        bool: True nếu tạo thành công hoặc đã tồn tại, False nếu thất bại
    """
    try:
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        # Kiểm tra nếu đã tồn tại
        if fs.exists(path):
            return True

        # Tạo thư mục
        success = fs.mkdirs(path)
        if success:
            print(f"✅ Đã tạo thành công thư mục HDFS: {hdfs_path}")
        else:
            print(f"❌ Không thể tạo thư mục HDFS: {hdfs_path}")

        return success

    except Exception as e:
        print(f"❌ Lỗi khi tạo thư mục HDFS {hdfs_path}: {e}")
        return False


def delete_hdfs_path(spark, hdfs_path, recursive=True):
    """
    Xóa đường dẫn HDFS

    Args:
        spark (SparkSession): SparkSession
        hdfs_path (str): Đường dẫn HDFS cần xóa
        recursive (bool): Xóa đệ quy (mặc định True)

    Returns:
        bool: True nếu xóa thành công, False nếu thất bại
    """
    try:
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            success = fs.delete(path, recursive)
            if success:
                print(f"✅ Đã xóa thành công: {hdfs_path}")
            else:
                print(f"❌ Không thể xóa: {hdfs_path}")
            return success
        else:
            print(f"⚠️ Đường dẫn không tồn tại: {hdfs_path}")
            return True  # Coi như thành công nếu đã không tồn tại

    except Exception as e:
        print(f"❌ Lỗi khi xóa đường dẫn HDFS {hdfs_path}: {e}")
        return False
