import os
import json
import logging
import tempfile
from datetime import datetime
from pyhdfs import HdfsClient

logger = logging.getLogger(__name__)


class HDFSWriter:
    def __init__(self):
        self.hdfs_hosts = os.environ.get("HDFS_HOSTS", "hdfs-namenode:9000").split(",")
        self.hdfs_user = os.environ.get("HDFS_USER", "hadoop")
        self.base_path = os.environ.get("HDFS_BASE_PATH", "/real_estate_data")

        # Kết nối tới HDFS
        try:
            self.client = HdfsClient(hosts=self.hdfs_hosts, user_name=self.hdfs_user)
            logger.info(f"Connected to HDFS at {self.hdfs_hosts}")

            # Tạo thư mục cơ sở nếu chưa tồn tại
            if not self.client.exists(self.base_path):
                self.client.mkdirs(self.base_path)
        except Exception as e:
            logger.error(f"Error connecting to HDFS: {e}")
            self.client = None

    def _get_path_for_date(self):
        """Tạo đường dẫn trên HDFS theo cấu trúc phân cấp theo ngày"""
        today = datetime.now().strftime("%Y/%m/%d")
        path = f"{self.base_path}/{today}"

        # Tạo thư mục nếu chưa tồn tại
        if not self.client.exists(path):
            self.client.mkdirs(path)

        return path

    def write_json(self, data, filename):
        """Ghi dữ liệu JSON vào HDFS"""
        if not self.client:
            logger.error("HDFS client not initialized")
            return False

        try:
            # Tạo tệp tin tạm thời cục bộ
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                json.dump(data, temp_file, ensure_ascii=False, indent=2)
                temp_file_path = temp_file.name

            # Đường dẫn đích trên HDFS
            hdfs_path = f"{self._get_path_for_date()}/{filename}"

            # Tải tệp tin lên HDFS
            self.client.copy_from_local(temp_file_path, hdfs_path)

            # Xóa tệp tin tạm thời
            os.unlink(temp_file_path)

            logger.info(f"Successfully wrote data to HDFS at {hdfs_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing to HDFS: {e}")
            return False

    def write_csv(self, df, filename):
        """Ghi DataFrame vào HDFS dưới dạng CSV"""
        if not self.client:
            logger.error("HDFS client not initialized")
            return False

        try:
            # Tạo tệp tin tạm thời cục bộ
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                df.to_csv(temp_file.name, index=False)
                temp_file_path = temp_file.name

            # Đường dẫn đích trên HDFS
            hdfs_path = f"{self._get_path_for_date()}/{filename}"

            # Tải tệp tin lên HDFS
            self.client.copy_from_local(temp_file_path, hdfs_path)

            # Xóa tệp tin tạm thời
            os.unlink(temp_file_path)

            logger.info(f"Successfully wrote CSV data to HDFS at {hdfs_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing CSV to HDFS: {e}")
            return False
