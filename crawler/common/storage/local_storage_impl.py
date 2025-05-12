import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any, Optional, Union
import logging

from common.base.base_storage import BaseStorage

logger = logging.getLogger(__name__)


class LocalStorage(BaseStorage):
    """
    Implementation của BaseStorage cho local storage
    """

    def __init__(self, base_path: str = "./data_output"):
        super().__init__(base_path=base_path)
        self._ensure_dir_exists()

    def _ensure_dir_exists(self):
        """Đảm bảo thư mục lưu trữ tồn tại"""
        os.makedirs(self.base_path, exist_ok=True)

    def save_data(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        file_name: Optional[str] = None,
        prefix: str = "data",
    ) -> str:
        """
        Lưu dữ liệu vào local storage

        Args:
            data: Dữ liệu cần lưu (list of dicts hoặc DataFrame)
            file_name: Tên file cụ thể (nếu None sẽ tự động tạo)
            prefix: Tiền tố cho tên file nếu tự động tạo

        Returns:
            str: Đường dẫn đến file đã lưu
        """
        self._ensure_dir_exists()

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

        # Tạo đường dẫn đầy đủ
        file_path = os.path.join(self.base_path, file_name)

        # Lưu dữ liệu theo định dạng
        if file_path.endswith(".csv"):
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
            logger.info(f"Saved {len(df)} records to CSV: {file_path}")
        else:  # Mặc định là parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)
            logger.info(f"Saved {len(df)} records to Parquet: {file_path}")

        return file_path

    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Đọc dữ liệu từ local storage

        Args:
            file_path: Đường dẫn đến file cần đọc

        Returns:
            pd.DataFrame: Dữ liệu đã đọc
        """
        if not self.file_exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_path.endswith(".csv"):
            return pd.read_csv(file_path)
        elif file_path.endswith(".parquet"):
            return pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")

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
        try:
            # Chuyển đổi dữ liệu thành DataFrame
            new_df = self._convert_to_dataframe(data)

            if self.file_exists(file_path):
                # Đọc dữ liệu hiện có
                existing_df = self.load_data(file_path)

                # Kết hợp dữ liệu
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)

                # Ghi lại file
                if file_path.endswith(".csv"):
                    combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
                else:  # Mặc định là parquet
                    table = pa.Table.from_pandas(combined_df)
                    pq.write_table(table, file_path)

                logger.info(f"Appended {len(new_df)} records to {file_path}")
                return True
            else:
                # File không tồn tại, tạo mới
                return self.save_data(new_df, os.path.basename(file_path)) is not None

        except Exception as e:
            logger.error(f"Error appending data to {file_path}: {e}")
            return False

    def list_files(
        self, prefix: Optional[str] = None, file_format: Optional[str] = None
    ) -> List[str]:
        """
        Liệt kê các file trong local storage

        Args:
            prefix: Tiền tố để lọc file
            file_format: Định dạng file để lọc

        Returns:
            List[str]: Danh sách đường dẫn đến các file
        """
        self._ensure_dir_exists()

        all_files = []

        for file in os.listdir(self.base_path):
            file_path = os.path.join(self.base_path, file)

            if not os.path.isfile(file_path):
                continue

            if prefix and not file.startswith(prefix):
                continue

            if file_format and not file.endswith(f".{file_format}"):
                continue

            all_files.append(file_path)

        return all_files

    def file_exists(self, file_path: str) -> bool:
        """
        Kiểm tra file có tồn tại không

        Args:
            file_path: Đường dẫn đến file cần kiểm tra

        Returns:
            bool: True nếu file tồn tại, False nếu không
        """
        return os.path.isfile(file_path)

    def get_links_from_file(self, file_path: str) -> List[str]:
        """
        Lấy danh sách các links từ file

        Args:
            file_path: Đường dẫn đến file

        Returns:
            List[str]: Danh sách các links
        """
        df = self.load_data(file_path)

        if "link" not in df.columns:
            raise ValueError(f"File {file_path} does not contain 'link' column")

        return df["link"].dropna().unique().tolist()
