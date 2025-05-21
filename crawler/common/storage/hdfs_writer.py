import os
import json
import logging
import tempfile
import numpy as np
import pandas as pd
from datetime import datetime
from pyhdfs import HdfsClient
import socket
import time
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
from typing import Dict, List, Any, Optional, Union

logger = logging.getLogger(__name__)


class HDFSWriter:
    """
    Lớp xử lý việc ghi dữ liệu vào HDFS
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
        self.namenode = namenode or os.environ.get("HDFS_NAMENODE", "namenode:9870")
        self.user = user or os.environ.get("HDFS_USER", "airflow")
        self.base_path = base_path or os.environ.get(
            "HDFS_BASE_PATH", "/data/realestate"
        )

        # Khởi tạo kết nối HDFS
        # Sử dụng thông số cơ bản để tránh version mismatch
        self.client = InsecureClient(f"http://{self.namenode}", user=self.user)
        logger.info(f"Initialized HDFS connection to {self.namenode}")

    def ensure_directory_exists(self, path: str) -> bool:
        """
        Đảm bảo thư mục tồn tại trên HDFS

        Args:
            path: Đường dẫn thư mục cần kiểm tra/tạo

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # If path is empty or just '/', return True as it should already exist
        if not path or path == "/":
            return True

        try:
            # Check if the directory exists first
            try:
                status = self.client.status(path, strict=False)
                if status is not None:
                    logger.info(f"Directory already exists: {path}")
                    return True
            except:
                pass

            # Try to create the directory
            logger.info(f"Creating directory on HDFS: {path}")
            self.client.makedirs(path)
            logger.info(f"Successfully created directory: {path}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory {path}: {e}")
            # Try to create parent directories recursively if needed
            try:
                parent_dir = os.path.dirname(path)
                if parent_dir and parent_dir != path:
                    if self.ensure_directory_exists(parent_dir):
                        self.client.makedirs(path)
                        logger.info(
                            f"Successfully created directory after creating parents: {path}"
                        )
                        return True
            except Exception as nested_e:
                logger.error(
                    f"Failed to create parent directories for {path}: {nested_e}"
                )
            return False

    def _convert_dataframe_to_string_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Chuyển đổi tất cả các cột của DataFrame sang kiểu string để tránh lỗi chuyển đổi kiểu

        Args:
            df: DataFrame cần chuyển đổi

        Returns:
            pd.DataFrame: DataFrame đã chuyển đổi
        """
        try:
            # First, replace NaN values with empty strings to avoid conversion errors
            df = df.fillna("")
            # Convert all columns to string type
            for col in df.columns:
                df[col] = df[col].astype(str)

            logger.info(
                f"Converted all columns to string type for DataFrame with {len(df)} rows"
            )
            return df
        except Exception as e:
            logger.warning(
                f"Error during data type conversion: {e}, attempting to continue anyway"
            )
            return df

    def write_dataframe_to_parquet(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Ghi DataFrame vào file Parquet trên HDFS

        Args:
            df: DataFrame cần lưu
            hdfs_path: Đường dẫn đích trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)

        # Đảm bảo thư mục cha tồn tại
        parent_dir = os.path.dirname(hdfs_path)
        self.ensure_directory_exists(parent_dir)

        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Lưu DataFrame vào file tạm
            table = pa.Table.from_pandas(df)
            pq.write_table(table, temp_path)

            # Upload lên HDFS
            self.client.upload(hdfs_path, temp_path, overwrite=True)
            logger.info(f"Saved DataFrame with {len(df)} rows to HDFS: {hdfs_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing DataFrame to HDFS: {e}")
            return False

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def write_dataframe_to_csv(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Ghi DataFrame vào file CSV trên HDFS

        Args:
            df: DataFrame cần lưu
            hdfs_path: Đường dẫn đích trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)

        # Đảm bảo thư mục cha tồn tại
        parent_dir = os.path.dirname(hdfs_path)
        self.ensure_directory_exists(parent_dir)

        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Lưu DataFrame vào file tạm
            df.to_csv(temp_path, index=False, encoding="utf-8")

            # Upload lên HDFS
            self.client.upload(hdfs_path, temp_path, overwrite=True)
            logger.info(
                f"Saved DataFrame with {len(df)} rows as CSV to HDFS: {hdfs_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Error writing DataFrame as CSV to HDFS: {e}")
            return False

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def write_dataframe_to_json(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Ghi DataFrame vào file JSON trên HDFS

        Args:
            df: DataFrame cần lưu
            hdfs_path: Đường dẫn đích trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)

        # Đảm bảo thư mục cha tồn tại
        parent_dir = os.path.dirname(hdfs_path)
        self.ensure_directory_exists(parent_dir)

        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Lưu DataFrame vào file tạm
            df.to_json(temp_path, orient="records", force_ascii=False, lines=True)

            # Upload lên HDFS
            self.client.upload(hdfs_path, temp_path, overwrite=True)
            logger.info(
                f"Saved DataFrame with {len(df)} rows as JSON to HDFS: {hdfs_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Error writing DataFrame as JSON to HDFS: {e}")
            return False

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def write_json_to_hdfs(self, data: Union[Dict, List], hdfs_path: str) -> bool:
        """
        Ghi dữ liệu JSON vào HDFS

        Args:
            data: Dữ liệu JSON cần lưu
            hdfs_path: Đường dẫn đích trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        import json

        # Đảm bảo thư mục cha tồn tại
        parent_dir = os.path.dirname(hdfs_path)
        self.ensure_directory_exists(parent_dir)

        try:
            # Chuyển đổi dữ liệu thành chuỗi JSON
            json_str = json.dumps(data, ensure_ascii=False)

            # Ghi vào HDFS
            with self.client.write(hdfs_path, overwrite=True) as writer:
                writer.write(json_str.encode("utf-8"))

            logger.info(f"Wrote JSON data to HDFS: {hdfs_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing JSON to HDFS: {e}")
            return False

    def read_parquet_from_hdfs(self, hdfs_path: str) -> Optional[pd.DataFrame]:
        """
        Đọc file Parquet từ HDFS

        Args:
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            Optional[pd.DataFrame]: DataFrame đã đọc hoặc None nếu thất bại
        """
        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Kiểm tra file tồn tại
            if not self.client.status(hdfs_path, strict=False):
                logger.error(f"File not found on HDFS: {hdfs_path}")
                return None

            # Download từ HDFS về local
            self.client.download(hdfs_path, temp_path, overwrite=True)

            # Đọc DataFrame
            df = pd.read_parquet(temp_path)
            logger.info(f"Read DataFrame with {len(df)} rows from HDFS: {hdfs_path}")
            return df

        except Exception as e:
            logger.error(f"Error reading Parquet from HDFS: {e}")
            return None

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def read_csv_from_hdfs(self, hdfs_path: str) -> Optional[pd.DataFrame]:
        """
        Đọc file CSV từ HDFS

        Args:
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            Optional[pd.DataFrame]: DataFrame đã đọc hoặc None nếu thất bại
        """
        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Kiểm tra file tồn tại
            if not self.client.status(hdfs_path, strict=False):
                logger.error(f"File not found on HDFS: {hdfs_path}")
                return None

            # Download từ HDFS về local
            self.client.download(hdfs_path, temp_path, overwrite=True)

            # Đọc DataFrame
            df = pd.read_csv(temp_path, encoding="utf-8")
            logger.info(
                f"Read DataFrame with {len(df)} rows from CSV on HDFS: {hdfs_path}"
            )
            return df

        except Exception as e:
            logger.error(f"Error reading CSV from HDFS: {e}")
            return None

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def read_json_from_hdfs(self, hdfs_path: str) -> Optional[pd.DataFrame]:
        """
        Đọc file JSON từ HDFS

        Args:
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            Optional[pd.DataFrame]: DataFrame đã đọc hoặc None nếu thất bại
        """
        # Tạo file tạm local
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Kiểm tra file tồn tại
            if not self.client.status(hdfs_path, strict=False):
                logger.error(f"File not found on HDFS: {hdfs_path}")
                return None

            # Download từ HDFS về local
            self.client.download(hdfs_path, temp_path, overwrite=True)

            # Đọc DataFrame
            df = pd.read_json(temp_path, orient="records", lines=True)
            logger.info(
                f"Read DataFrame with {len(df)} rows from JSON on HDFS: {hdfs_path}"
            )
            return df

        except Exception as e:
            logger.error(f"Error reading JSON from HDFS: {e}")
            return None

        finally:
            # Xóa file tạm
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def append_dataframe_to_parquet(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Thêm DataFrame vào file Parquet hiện có

        Args:
            df: DataFrame cần thêm
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)
        try:
            # Kiểm tra file tồn tại
            file_exists = self.client.status(hdfs_path, strict=False) is not None

            if file_exists:
                # Đọc DataFrame hiện có
                existing_df = self.read_parquet_from_hdfs(hdfs_path)
                if existing_df is None:
                    logger.warning(
                        f"Could not read existing file, creating new one: {hdfs_path}"
                    )
                    return self.write_dataframe_to_parquet(df, hdfs_path)

                # Gộp DataFrame
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Ghi lại file
                return self.write_dataframe_to_parquet(combined_df, hdfs_path)
            else:
                # File không tồn tại, tạo mới
                return self.write_dataframe_to_parquet(df, hdfs_path)

        except Exception as e:
            logger.error(f"Error appending DataFrame to HDFS file: {e}")
            return False

    def append_dataframe_to_csv(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Thêm DataFrame vào file CSV hiện có

        Args:
            df: DataFrame cần thêm
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)
        try:
            # Kiểm tra file tồn tại
            file_exists = self.client.status(hdfs_path, strict=False) is not None

            if file_exists:
                # Đọc DataFrame hiện có
                existing_df = self.read_csv_from_hdfs(hdfs_path)
                if existing_df is None:
                    logger.warning(
                        f"Could not read existing file, creating new one: {hdfs_path}"
                    )
                    return self.write_dataframe_to_csv(df, hdfs_path)

                # Gộp DataFrame
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Ghi lại file
                return self.write_dataframe_to_csv(combined_df, hdfs_path)
            else:
                # File không tồn tại, tạo mới
                return self.write_dataframe_to_csv(df, hdfs_path)

        except Exception as e:
            logger.error(f"Error appending DataFrame to CSV file on HDFS: {e}")
            return False

    def append_dataframe_to_json(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Thêm DataFrame vào file JSON hiện có

        Args:
            df: DataFrame cần thêm
            hdfs_path: Đường dẫn file trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        # Chuyển đổi tất cả các cột sang kiểu string
        df = self._convert_dataframe_to_string_types(df)
        try:
            # Kiểm tra file tồn tại
            file_exists = self.client.status(hdfs_path, strict=False) is not None

            if file_exists:
                # Đọc DataFrame hiện có
                existing_df = self.read_json_from_hdfs(hdfs_path)
                if existing_df is None:
                    logger.warning(
                        f"Could not read existing file, creating new one: {hdfs_path}"
                    )
                    return self.write_dataframe_to_json(df, hdfs_path)

                # Gộp DataFrame
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Ghi lại file
                return self.write_dataframe_to_json(combined_df, hdfs_path)
            else:
                # File không tồn tại, tạo mới
                return self.write_dataframe_to_json(df, hdfs_path)

        except Exception as e:
            logger.error(f"Error appending DataFrame to JSON file on HDFS: {e}")
            return False

    def list_files(self, hdfs_dir: str, pattern: str = None) -> List[str]:
        """
        Liệt kê các file trong thư mục HDFS

        Args:
            hdfs_dir: Thư mục HDFS cần liệt kê
            pattern: Mẫu để lọc tên file (ví dụ: *.parquet)

        Returns:
            List[str]: Danh sách đường dẫn file
        """
        try:
            # Đảm bảo thư mục tồn tại
            if not self.client.status(hdfs_dir, strict=False):
                logger.warning(f"Directory not found on HDFS: {hdfs_dir}")
                return []

            # Liệt kê các file
            files = self.client.list(hdfs_dir, status=False)

            # Lọc theo pattern nếu có
            if pattern:
                import fnmatch

                files = [f for f in files if fnmatch.fnmatch(f, pattern)]

            # Trả về đường dẫn đầy đủ
            return [os.path.join(hdfs_dir, f) for f in files]

        except Exception as e:
            logger.error(f"Error listing files in HDFS directory {hdfs_dir}: {e}")
            return []

    def generate_file_path(self, prefix: str = "data", format: str = "parquet") -> str:
        """
        Tạo đường dẫn file với timestamp

        Args:
            prefix: Tiền tố cho tên file
            format: Định dạng file

        Returns:
            str: Đường dẫn file đã tạo
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.{format}"
        return os.path.join(self.base_path, filename)

    def write_dataframe_to_avro(self, df: pd.DataFrame, hdfs_path: str) -> bool:
        """
        Ghi DataFrame vào file Avro trên HDFS

        Args:
            df: DataFrame cần lưu
            hdfs_path: Đường dẫn đích trên HDFS

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Đảm bảo thư mục cha tồn tại
            parent_dir = os.path.dirname(hdfs_path)
            self.ensure_directory_exists(parent_dir)

            # Tạo file tạm local
            with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as temp_file:
                temp_path = temp_file.name

            try:
                # Chuyển đổi các giá trị trong DataFrame thành một list các dict
                records = df.to_dict("records")

                try:
                    import fastavro

                    # Tạo schema Avro từ DataFrame
                    schema = {
                        "namespace": "realestate.avro",
                        "type": "record",
                        "name": "Property",
                        "fields": [
                            {"name": col, "type": ["null", "string"], "default": None}
                            for col in df.columns
                        ],
                    }

                    # Convert data to strings
                    for record in records:
                        for key, value in record.items():
                            if value is not None and not isinstance(value, str):
                                record[key] = str(value)

                    # Write to Avro file
                    with open(temp_path, "wb") as out_file:
                        fastavro.writer(out_file, schema, records)

                except ImportError:
                    # Fallback to simple JSON if fastavro is not available
                    logger.warning(
                        "fastavro not available, falling back to JSON format"
                    )
                    with open(temp_path, "w") as out_file:
                        json.dump(records, out_file)
                    # Rename to indicate it's actually JSON
                    hdfs_path = hdfs_path.replace(".avro", ".json")

                # Upload lên HDFS
                self.client.upload(hdfs_path, temp_path, overwrite=True)
                logger.info(f"Saved DataFrame with {len(df)} rows to HDFS: {hdfs_path}")
                return True

            except Exception as e:
                logger.error(f"Error writing DataFrame to HDFS: {e}")
                return False

            finally:
                # Xóa file tạm
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        except Exception as e:
            logger.error(f"Error in Avro writer: {e}. Falling back to JSON.")
            return self.write_dataframe_to_json(df, hdfs_path.replace(".avro", ".json"))
