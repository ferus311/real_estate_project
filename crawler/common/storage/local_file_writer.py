import os
import json
import logging
import pandas as pd
from datetime import datetime
import math

logger = logging.getLogger(__name__)


class LocalFileWriter:
    def __init__(self):
        # Đường dẫn lưu trữ dữ liệu cục bộ
        self.base_path = os.environ.get(
            "LOCAL_OUTPUT_PATH", "/home/fer/data/property_data"
        )

        # Đảm bảo rằng thư mục đầu ra tồn tại
        self._ensure_output_dir_exists()

        # Cấu hình kích thước file tối ưu (byte) - mặc định 128MB
        self.target_file_size = int(
            os.environ.get("TARGET_FILE_SIZE", 128 * 1024 * 1024)
        )

        # Định dạng file mặc định (parquet hoặc csv)
        self.default_format = os.environ.get("DEFAULT_FORMAT", "csv")

        logger.info(f"LocalFileWriter initialized with base path: {self.base_path}")

    def _ensure_output_dir_exists(self):
        """Đảm bảo thư mục đầu ra tồn tại"""
        try:
            os.makedirs(self.base_path, exist_ok=True)
            # Tạo thêm thư mục theo ngày nếu cần
            daily_path = self._get_path_for_date()
            os.makedirs(daily_path, exist_ok=True)
            logger.info(f"Output directory created/verified: {daily_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating output directory: {e}")
            return False

    def _get_path_for_date(self):
        """Tạo đường dẫn theo cấu trúc phân cấp theo ngày"""
        today = datetime.now().strftime("%Y/%m/%d")
        path = os.path.join(self.base_path, today.replace("/", os.sep))
        return path

    def _estimate_file_size(self, df):
        """Ước tính kích thước DataFrame để quyết định có nên chia file hay không"""
        # Phương pháp ước lượng đơn giản dựa trên memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        logger.info(f"Estimated DataFrame size: {memory_usage} bytes")
        return memory_usage

    def _split_dataframe(self, df, max_size):
        """Chia DataFrame thành các phần nhỏ hơn dựa trên kích thước ước tính"""
        estimated_size = self._estimate_file_size(df)

        if estimated_size <= max_size or len(df) <= 1:
            return [df]

        num_chunks = math.ceil(estimated_size / max_size)
        chunk_size = math.ceil(len(df) / num_chunks)

        logger.info(
            f"Splitting DataFrame of size {estimated_size} bytes into {num_chunks} chunks"
        )

        return [df[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

    def write_json(self, data, filename):
        """Ghi dữ liệu JSON vào thư mục cục bộ"""
        try:
            # Đảm bảo thư mục đầu ra tồn tại
            daily_path = self._get_path_for_date()
            os.makedirs(daily_path, exist_ok=True)

            # Đường dẫn đầy đủ đến file
            file_path = os.path.join(daily_path, filename)

            # Ghi dữ liệu vào file JSON
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Successfully wrote data to local file at {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing to local JSON file: {e}")
            return False

    def write_csv(self, df, filename):
        """Ghi DataFrame vào thư mục cục bộ dưới dạng CSV"""
        try:
            # Kiểm tra kích thước và chia DataFrame nếu cần
            chunks = self._split_dataframe(df, self.target_file_size)

            successful_writes = 0

            for i, chunk in enumerate(chunks):
                # Tạo tên file phù hợp nếu có nhiều phần
                chunk_filename = (
                    filename
                    if len(chunks) == 1
                    else f"{os.path.splitext(filename)[0]}_part{i}{os.path.splitext(filename)[1]}"
                )

                # Đảm bảo thư mục đầu ra tồn tại
                daily_path = self._get_path_for_date()
                os.makedirs(daily_path, exist_ok=True)

                # Đường dẫn đầy đủ đến file
                file_path = os.path.join(daily_path, chunk_filename)

                # Ghi DataFrame ra file CSV
                chunk.to_csv(file_path, index=False, encoding="utf-8")

                successful_writes += 1
                logger.info(
                    f"Successfully wrote CSV chunk {i+1}/{len(chunks)} to {file_path}"
                )

            return successful_writes == len(chunks)

        except Exception as e:
            logger.error(f"Error writing to local CSV file: {e}")
            return False

    def write_parquet(self, df, filename):
        """Ghi DataFrame vào thư mục cục bộ dưới dạng Parquet"""
        try:
            # Đảm bảo filename có đuôi .parquet
            if not filename.endswith(".parquet"):
                filename = f"{os.path.splitext(filename)[0]}.parquet"

            # Kiểm tra kích thước và chia DataFrame nếu cần
            chunks = self._split_dataframe(df, self.target_file_size)

            successful_writes = 0

            for i, chunk in enumerate(chunks):
                # Tạo tên file phù hợp nếu có nhiều phần
                chunk_filename = (
                    filename
                    if len(chunks) == 1
                    else f"{os.path.splitext(filename)[0]}_part{i}.parquet"
                )

                # Đảm bảo thư mục đầu ra tồn tại
                daily_path = self._get_path_for_date()
                os.makedirs(daily_path, exist_ok=True)

                # Đường dẫn đầy đủ đến file
                file_path = os.path.join(daily_path, chunk_filename)

                # Ghi DataFrame ra file Parquet với nén snappy
                chunk.to_parquet(
                    file_path,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )

                successful_writes += 1
                logger.info(
                    f"Successfully wrote Parquet chunk {i+1}/{len(chunks)} to {file_path}"
                )

            return successful_writes == len(chunks)

        except Exception as e:
            logger.error(f"Error writing to local Parquet file: {e}")
            return False

    def write(self, df, filename, format=None):
        """
        Phương thức tiện ích để ghi dữ liệu theo định dạng mặc định hoặc được chỉ định
        """
        if format is None:
            format = self.default_format

        if format.lower() == "parquet":
            return self.write_parquet(df, filename)
        elif format.lower() == "csv":
            return self.write_csv(df, filename)
        elif format.lower() == "json":
            return self.write_json(df.to_dict("records"), filename)
        else:
            logger.error(
                f"Unsupported file format: {format}. Using default format {self.default_format}"
            )
            return self.write(df, filename, self.default_format)
