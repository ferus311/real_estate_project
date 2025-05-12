import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any, Optional, Union
import logging
import tempfile
from hdfs import InsecureClient
from datetime import datetime

from common.base.base_storage import BaseStorage
from common.storage.hdfs_writer import HDFSWriter

logger = logging.getLogger(__name__)


class HDFSStorage(BaseStorage):
    """
    Implementation của BaseStorage cho HDFS storage
    """

    def __init__(self, namenode: str = None, user: str = None, base_path: str = None):
        """
        Khởi tạo kết nối đến HDFS

        Args:
            namenode: Địa chỉ namenode (host:port)
            user: Tên user HDFS
            base_path: Đường dẫn thư mục cơ sở trên HDFS
        """
        # Lấy thông tin từ biến môi trường nếu không được cung cấp
        namenode = namenode or os.environ.get("HDFS_NAMENODE", "namenode:9870")
        user = user or os.environ.get("HDFS_USER", "airflow")
        base_path = base_path or os.environ.get("HDFS_BASE_PATH", "/data/realestate")

        super().__init__(base_path=base_path)

        # Sử dụng HDFSWriter cho các thao tác HDFS
        self.writer = HDFSWriter(namenode=namenode, user=user, base_path=base_path)
        self.namenode = namenode
        self.user = user

        logger.info(f"Initialized HDFS storage with connection to {namenode}")

    def save_data(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        file_name: Optional[str] = None,
        prefix: str = "data",
    ) -> str:
        """
        Lưu dữ liệu vào HDFS

        Args:
            data: Dữ liệu cần lưu (list of dicts hoặc DataFrame)
            file_name: Tên file cụ thể (nếu None sẽ tự động tạo)
            prefix: Tiền tố cho tên file nếu tự động tạo

        Returns:
            str: Đường dẫn đến file đã lưu trên HDFS
        """
        if not data:
            logger.warning("No data to save")
            return None

        # Chuyển đổi dữ liệu thành DataFrame nếu cần
        df = self._convert_to_dataframe(data)

        # Tạo tên file nếu không được cung cấp
        if not file_name:
            file_name = self.generate_file_name(prefix, "parquet")

        # Đảm bảo file_name có định dạng đúng
        if not file_name.endswith(".parquet") and not file_name.endswith(".csv"):
            file_name += ".parquet"

        # Tạo đường dẫn đầy đủ trên HDFS
        hdfs_path = os.path.join(self.base_path, file_name)

        # Sử dụng HDFSWriter để ghi DataFrame vào HDFS
        success = self.writer.write_dataframe_to_parquet(df, hdfs_path)

        if success:
            return hdfs_path
        return None

    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Đọc dữ liệu từ HDFS

        Args:
            file_path: Đường dẫn đến file cần đọc trên HDFS

        Returns:
            pd.DataFrame: Dữ liệu đã đọc
        """
        if not self.file_exists(file_path):
            raise FileNotFoundError(f"File not found on HDFS: {file_path}")

        # Sử dụng HDFSWriter để đọc DataFrame từ HDFS
        df = self.writer.read_parquet_from_hdfs(file_path)

        if df is None:
            raise IOError(f"Failed to read data from HDFS: {file_path}")

        return df

    def append_data(
        self, data: Union[List[Dict[str, Any]], pd.DataFrame], file_path: str
    ) -> bool:
        """
        Thêm dữ liệu vào file hiện có trên HDFS

        Args:
            data: Dữ liệu cần thêm
            file_path: Đường dẫn đến file cần thêm trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi dữ liệu mới thành DataFrame
        new_df = self._convert_to_dataframe(data)

        # Sử dụng HDFSWriter để append DataFrame vào file hiện có
        return self.writer.append_dataframe_to_parquet(new_df, file_path)

    def list_files(
        self, prefix: Optional[str] = None, file_format: Optional[str] = None
    ) -> List[str]:
        """
        Liệt kê các file trong storage

        Args:
            prefix: Tiền tố để lọc file
            file_format: Định dạng file để lọc

        Returns:
            List[str]: Danh sách đường dẫn đến các file
        """
        pattern = None

        # Tạo pattern từ prefix và file_format nếu có
        if prefix and file_format:
            pattern = f"{prefix}*.{file_format}"
        elif prefix:
            pattern = f"{prefix}*"
        elif file_format:
            pattern = f"*.{file_format}"

        # Sử dụng HDFSWriter để liệt kê files
        return self.writer.list_files(self.base_path, pattern)

    def file_exists(self, file_path: str) -> bool:
        """
        Kiểm tra file có tồn tại không

        Args:
            file_path: Đường dẫn đến file cần kiểm tra

        Returns:
            bool: True nếu file tồn tại, False nếu không
        """
        try:
            # Kiểm tra file tồn tại bằng cách lấy status
            return self.writer.client.status(file_path, strict=False) is not None
        except Exception as e:
            logger.error(f"Error checking if file exists: {e}")
            return False

    def get_links_from_file(self, file_path: str) -> List[str]:
        """
        Đọc danh sách links từ file trên HDFS

        Args:
            file_path: Đường dẫn đến file trên HDFS

        Returns:
            List[str]: Danh sách các links
        """
        try:
            # Đọc DataFrame
            df = self.load_data(file_path)

            # Kiểm tra cột 'url' hoặc 'link' có tồn tại không
            if "url" in df.columns:
                return df["url"].tolist()
            elif "link" in df.columns:
                return df["link"].tolist()
            else:
                logger.warning(f"No url or link column found in file: {file_path}")
                return []

        except Exception as e:
            logger.error(f"Error getting links from file: {e}")
            return []
