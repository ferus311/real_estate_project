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

        # Log connection information
        logger.info(
            f"Initializing HDFS storage with namenode: {namenode}, user: {user}, base_path: {base_path}"
        )

        try:
            # Sử dụng HDFSWriter cho các thao tác HDFS
            self.writer = HDFSWriter(namenode=namenode, user=user, base_path=base_path)
            self.namenode = namenode
            self.user = user

            # Test connection by checking if base path exists
            if not self.writer.ensure_directory_exists(base_path):
                logger.warning(
                    f"Could not create or verify base path {base_path} on HDFS. Check your connection parameters."
                )
        except Exception as e:
            logger.error(f"Error initializing HDFS connection: {e}")
            # Still initialize the writer, but log the error for troubleshooting
            self.writer = HDFSWriter(namenode=namenode, user=user, base_path=base_path)
            self.namenode = namenode
            self.user = user

        logger.info(f"Initialized HDFS storage with connection to {namenode}")

    def build_path_for_raw_data(self, source: str, data_type: str) -> str:
        """
        Xây dựng đường dẫn cho dữ liệu thô theo kiến trúc thư mục mới

        Args:
            source: Nguồn dữ liệu (ví dụ: batdongsan, chotot)
            data_type: Loại dữ liệu (ví dụ: list, detail, api)

        Returns:
            str: Đường dẫn thư mục cho dữ liệu thô
        """
        # Lấy năm và tháng hiện tại
        now = datetime.now()
        year, month = now.strftime("%Y"), now.strftime("%m")

        # Tạo đường dẫn theo cấu trúc: /realestate/raw/{source}/{data_type}/{year}/{month}/
        raw_path = f"/realestate/raw/{source}/{data_type}/{year}/{month}"

        # Đảm bảo thư mục tồn tại
        self.writer.ensure_directory_exists(raw_path)

        return raw_path

    def build_path_for_processed_data(
        self, process_type: str, source: str = None, category: str = None
    ) -> str:
        """
        Xây dựng đường dẫn cho dữ liệu đã xử lý theo kiến trúc thư mục mới

        Args:
            process_type: Loại xử lý (ví dụ: cleaned, integrated, analytics)
            source: Nguồn dữ liệu (tùy chọn, ví dụ: batdongsan, chotot)
            category: Danh mục phân tích (tùy chọn, ví dụ: price_trends, region_stats)

        Returns:
            str: Đường dẫn thư mục cho dữ liệu đã xử lý
        """
        # Lấy năm và tháng hiện tại
        now = datetime.now()
        year, month = now.strftime("%Y"), now.strftime("%m")

        # Xây dựng đường dẫn dựa vào loại xử lý
        if process_type == "analytics" and category:
            # Ví dụ: /realestate/processed/analytics/price_trends/2025/05/
            path = f"/realestate/processed/{process_type}/{category}/{year}/{month}"
        elif source:
            # Ví dụ: /realestate/processed/cleaned/2025/05/batdongsan/
            path = f"/realestate/processed/{process_type}/{year}/{month}/{source}"
        else:
            # Ví dụ: /realestate/processed/integrated/2025/05/
            path = f"/realestate/processed/{process_type}/{year}/{month}"

        # Đảm bảo thư mục tồn tại
        self.writer.ensure_directory_exists(path)

        return path

    def _choose_optimal_format_for_path(
        self, file_path: str, data_type: str = "detail"
    ) -> str:
        """
        Xác định định dạng lưu trữ tối ưu dựa vào đường dẫn và loại dữ liệu

        Args:
            file_path: Đường dẫn file
            data_type: Loại dữ liệu (detail, list, api)

        Returns:
            str: Định dạng lưu trữ phù hợp (parquet, json, avro, csv)
        """
        # Nếu đường dẫn chứa '/raw/' thì sử dụng JSON cho dữ liệu thô
        if "/raw/" in file_path:
            # Dữ liệu thô (raw) lưu dưới dạng JSON để dễ đọc và tương thích tốt
            return "json"

        # Nếu đường dẫn chứa '/processed/' thì sử dụng định dạng tối ưu cho phân tích
        elif "/processed/" in file_path:
            # Dữ liệu đã xử lý nên lưu dưới dạng Parquet để phân tích hiệu quả
            return "parquet"

        # Nếu đường dẫn chứa '/ml/' thì sử dụng định dạng phù hợp cho ML
        elif "/ml/" in file_path:
            # Dữ liệu ML cũng nên lưu dưới dạng Parquet
            return "parquet"

        # Nếu không xác định được, sử dụng định dạng mặc định dựa vào loại dữ liệu
        elif data_type == "detail":
            # Chi tiết bất động sản có nhiều trường, nên dùng Parquet
            return "parquet"
        elif data_type == "list":
            # Danh sách URL có thể dùng CSV đơn giản
            return "csv"
        else:
            # Mặc định sử dụng JSON cho khả năng tương thích cao
            return "json"

    def save_data(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        file_name: Optional[str] = None,
        prefix: str = "data",
        file_format: str = "auto",
    ) -> str:
        """
        Lưu dữ liệu vào HDFS

        Args:
            data: Dữ liệu cần lưu (list of dicts hoặc DataFrame)
            file_name: Tên file cụ thể (nếu None sẽ tự động tạo)
            prefix: Tiền tố cho tên file nếu tự động tạo
            file_format: Định dạng file để lưu (parquet, csv, json, avro, auto)

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
            file_name = self.generate_file_name(prefix, file_format)

        # Xác định loại dữ liệu (detail, list, api) dựa vào tên file
        data_type = "detail"  # Mặc định
        if "list" in file_name:
            data_type = "list"
        elif "api" in file_name:
            data_type = "api"

        # Xác định định dạng file tối ưu nếu định dạng là "auto"
        if file_format == "auto":
            file_format = self._choose_optimal_format_for_path(file_name, data_type)

        # Đảm bảo file_name có đuôi phù hợp với định dạng
        if not file_name.endswith(f".{file_format}"):
            file_name += f".{file_format}"

        # Tạo đường dẫn đầy đủ trên HDFS
        hdfs_path = os.path.join(self.base_path, file_name)

        # Sử dụng HDFSWriter để ghi DataFrame vào HDFS theo định dạng
        success = False

        if file_format == "parquet":
            success = self.writer.write_dataframe_to_parquet(df, hdfs_path)
        elif file_format == "csv":
            success = self.writer.write_dataframe_to_csv(df, hdfs_path)
        elif file_format == "json":
            success = self.writer.write_dataframe_to_json(df, hdfs_path)
        elif file_format == "avro":
            success = self.writer.write_dataframe_to_avro(df, hdfs_path)
        else:
            logger.warning(
                f"Unsupported file format: {file_format}, using parquet instead"
            )
            # Sửa lại đuôi file
            hdfs_path = hdfs_path.rsplit(".", 1)[0] + ".parquet"
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

        # Xác định định dạng file từ đuôi file
        file_format = file_path.rsplit(".", 1)[-1].lower()

        # Đọc dữ liệu theo định dạng file
        df = None
        if file_format == "parquet":
            df = self.writer.read_parquet_from_hdfs(file_path)
        elif file_format == "csv":
            df = self.writer.read_csv_from_hdfs(file_path)
        elif file_format == "json":
            df = self.writer.read_json_from_hdfs(file_path)
        else:
            logger.warning(f"Unsupported file format: {file_format}, trying as parquet")
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
        # Lưu ý: Không cần chuyển đổi kiểu ở đây vì HDFSWriter sẽ thực hiện việc này

        # Xác định định dạng file từ đuôi file
        file_format = file_path.rsplit(".", 1)[-1].lower()

        # Append dữ liệu theo định dạng file
        if file_format == "parquet":
            return self.writer.append_dataframe_to_parquet(new_df, file_path)
        elif file_format == "csv":
            return self.writer.append_dataframe_to_csv(new_df, file_path)
        elif file_format == "json":
            return self.writer.append_dataframe_to_json(new_df, file_path)
        else:
            logger.warning(
                f"Unsupported file format for append: {file_format}, trying as parquet"
            )
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
