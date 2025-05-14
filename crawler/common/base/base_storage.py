from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from datetime import datetime


class BaseStorage(ABC):
    """
    Lớp cơ sở cho tất cả các storage
    """

    def __init__(self, base_path: str = None):
        self.base_path = base_path

    @abstractmethod
    def save_data(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        file_name: Optional[str] = None,
        prefix: str = "data",
        file_format: str = "parquet",
    ) -> str:
        """
        Lưu dữ liệu vào storage

        Args:
            data: Dữ liệu cần lưu (list of dicts hoặc DataFrame)
            file_name: Tên file cụ thể (nếu None sẽ tự động tạo)
            prefix: Tiền tố cho tên file nếu tự động tạo
            file_format: Định dạng file để lưu (parquet, csv, json)

        Returns:
            str: Đường dẫn đến file đã lưu
        """
        pass

    @abstractmethod
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Đọc dữ liệu từ storage

        Args:
            file_path: Đường dẫn đến file cần đọc

        Returns:
            pd.DataFrame: Dữ liệu đã đọc
        """
        pass

    @abstractmethod
    def append_data(
        self, data: Union[List[Dict[str, Any]], pd.DataFrame], file_path: str
    ) -> bool:
        """
        Thêm dữ liệu vào file hiện có

        Args:
            data: Dữ liệu cần thêm
            file_path: Đường dẫn đến file cần thêm

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """
        Kiểm tra file có tồn tại không

        Args:
            file_path: Đường dẫn đến file cần kiểm tra

        Returns:
            bool: True nếu file tồn tại, False nếu không
        """
        pass

    def generate_file_name(
        self, prefix: str = "data", file_format: str = "parquet"
    ) -> str:
        """
        Tạo tên file với timestamp

        Args:
            prefix: Tiền tố cho tên file
            file_format: Định dạng file

        Returns:
            str: Tên file đã tạo
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}_{timestamp}.{file_format}"

    def _convert_to_dataframe(
        self, data: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Chuyển đổi dữ liệu thành DataFrame

        Args:
            data: Dữ liệu cần chuyển đổi

        Returns:
            pd.DataFrame: DataFrame đã chuyển đổi
        """
        if isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            raise ValueError("Data must be a list of dictionaries or a DataFrame")

    def get_latest_file(
        self, prefix: str = "data", file_format: str = "parquet"
    ) -> Optional[str]:
        """
        Lấy file mới nhất theo prefix và format

        Args:
            prefix: Tiền tố để lọc file
            file_format: Định dạng file để lọc

        Returns:
            str: Đường dẫn đến file mới nhất, hoặc None nếu không có
        """
        files = self.list_files(prefix, file_format)
        if not files:
            return None

        # Sắp xếp theo thời gian tạo (giả định tên file có timestamp)
        files.sort(reverse=True)
        return files[0]
