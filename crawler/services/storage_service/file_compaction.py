import os
import sys
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
import re
import time

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.utils.logging_utils import setup_logging
from common.factory.storage_factory import StorageFactory
from common.storage.hdfs_storage_impl import HDFSStorage
from dotenv import load_dotenv

load_dotenv()
logger = setup_logging()


class FileCompactionService:
    """
    Service quản lý việc gộp (compact) các file nhỏ thành file lớn trên HDFS
    """

    def __init__(self):
        # Cấu hình từ biến môi trường
        self.storage_type = os.environ.get("STORAGE_TYPE", "hdfs")
        self.min_compact_files = int(os.environ.get("MIN_COMPACT_FILES", "5"))
        self.target_file_size_mb = int(os.environ.get("TARGET_FILE_SIZE_MB", "128"))
        self.max_file_age_days = int(os.environ.get("MAX_FILE_AGE_DAYS", "7"))
        self.file_prefix = os.environ.get("FILE_PREFIX", "property_data")
        self.file_format = os.environ.get(
            "FILE_FORMAT", "parquet"
        )  # parquet, csv, json

        # Khởi tạo storage (phải là HDFS)
        if self.storage_type != "hdfs":
            logger.warning(
                f"Storage type {self.storage_type} is not supported for compaction. Using HDFS instead."
            )
            self.storage_type = "hdfs"

        self.storage = StorageFactory.create_storage(self.storage_type)
        if not isinstance(self.storage, HDFSStorage):
            raise ValueError("FileCompactionService requires HDFSStorage")

        self.base_path = self.storage.base_path
        logger.info(
            f"File Compaction Service initialized with base path: {self.base_path}"
        )
        logger.info(
            f"Using file format: {self.file_format}, file prefix: {self.file_prefix}"
        )

    def list_sources(self):
        """
        Liệt kê tất cả các nguồn dữ liệu

        Returns:
            List[str]: Danh sách các nguồn dữ liệu
        """
        sources = []
        try:
            # Liệt kê tất cả các thư mục trong base_path
            directories = self.storage.writer.list_files(self.base_path, pattern=None)

            for directory in directories:
                # Kiểm tra nếu là thư mục (có ít nhất 1 file với định dạng yêu cầu)
                if self.storage.writer.list_files(
                    directory, pattern=f"*.{self.file_format}"
                ):
                    sources.append(directory)

            return sources
        except Exception as e:
            logger.error(f"Error listing sources: {e}")
            return []

    def find_compaction_candidates(self, source_path, min_days=0, max_days=None):
        """
        Tìm các nhóm file cần compact

        Args:
            source_path: Đường dẫn của nguồn
            min_days: Số ngày tối thiểu từ ngày hiện tại
            max_days: Số ngày tối đa từ ngày hiện tại

        Returns:
            Dict[str, List[str]]: Dictionary của các thư mục và danh sách file trong đó
        """
        max_days = max_days or self.max_file_age_days

        # Tạo đối tượng datetime cho các ngày cần kiểm tra
        today = datetime.now()
        min_date = today - timedelta(days=max_days)
        max_date = today - timedelta(days=min_days)

        # Regex để extract ngày từ tên file, ví dụ: property_data_2023_04_15_120000.parquet
        date_pattern = r"_(\d{4})_(\d{2})_(\d{2})_"

        candidates = {}

        # Liệt kê tất cả các file với định dạng cụ thể
        files = self.storage.writer.list_files(
            source_path, pattern=f"*.{self.file_format}"
        )

        # Gom nhóm file theo ngày
        files_by_date = {}

        for file_path in files:
            # Extract date từ tên file
            file_name = os.path.basename(file_path)
            match = re.search(date_pattern, file_name)
            if not match:
                continue

            year, month, day = match.groups()
            file_date_str = f"{year}-{month}-{day}"

            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

                # Kiểm tra nếu file nằm trong khoảng thời gian cần compact
                if min_date <= file_date <= max_date:
                    # Dùng ngày làm key để gom nhóm
                    date_key = file_date.strftime("%Y_%m_%d")

                    if date_key not in files_by_date:
                        files_by_date[date_key] = []

                    files_by_date[date_key].append(file_path)
            except ValueError:
                logger.warning(f"Could not parse date from {file_path}")

        # Với mỗi ngày, nếu có đủ file, thêm vào danh sách candidates
        for date_key, date_files in files_by_date.items():
            if len(date_files) >= self.min_compact_files:
                candidate_key = f"{source_path}/{date_key}"
                candidates[candidate_key] = date_files

        return candidates

    def compact_files(self, directory_key, files):
        """
        Gộp danh sách file thành một file lớn

        Args:
            directory_key: Khóa thư mục (nguồn/ngày)
            files: Danh sách file cần gộp

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if not files:
            return False

        try:
            # Đọc tất cả các file vào DataFrame
            dfs = []
            total_size = 0

            for file_path in files:
                try:
                    # Gọi hàm load_data đã được định nghĩa trong storage, hỗ trợ format khác nhau
                    df = self.storage.load_data(file_path)
                    dfs.append(df)

                    # Tính toán kích thước tương đối của DataFrame
                    size_bytes = df.memory_usage(deep=True).sum()
                    size_mb = size_bytes / (1024 * 1024)
                    total_size += size_mb

                    logger.debug(f"Read file {file_path} with size ~{size_mb:.2f}MB")
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {e}")

            if not dfs:
                logger.warning(f"No valid files to compact for {directory_key}")
                return False

            # Gộp tất cả DataFrame
            combined_df = pd.concat(dfs, ignore_index=True)

            # Tạo tên file mới
            # Phân tách directory_key để lấy thông tin nguồn và ngày
            parts = directory_key.split("/")
            source = parts[-2]  # Nguồn dữ liệu
            date_str = parts[-1]  # Ngày dạng YYYY_MM_DD

            # Tạo tên file mới với định dạng đúng
            new_file_name = (
                f"{self.file_prefix}_compacted_{date_str}.{self.file_format}"
            )

            # Thư mục đích là thư mục nguồn
            directory = os.path.dirname(files[0])
            new_file_path = os.path.join(directory, new_file_name)

            # Lưu DataFrame đã gộp với định dạng chỉ định
            saved_path = self.storage.save_data(
                data=combined_df, file_name=new_file_path, file_format=self.file_format
            )

            if saved_path:
                logger.info(
                    f"Successfully compacted {len(files)} files into {saved_path} with total size ~{total_size:.2f}MB"
                )

                # Xóa các file gốc sau khi compact thành công
                for file_path in files:
                    try:
                        self.storage.writer.client.delete(file_path)
                        logger.debug(f"Deleted original file: {file_path}")
                    except Exception as e:
                        logger.error(f"Error deleting original file {file_path}: {e}")

                return True
            else:
                logger.error(f"Failed to save compacted file to {new_file_path}")
                return False

        except Exception as e:
            logger.error(f"Error compacting files for {directory_key}: {e}")
            return False

    def run_compaction(self, min_days=1, max_days=None):
        """
        Chạy quá trình compact

        Args:
            min_days: Số ngày tối thiểu từ ngày hiện tại để compact
            max_days: Số ngày tối đa từ ngày hiện tại để compact

        Returns:
            int: Số thư mục đã compact thành công
        """
        try:
            # Lấy danh sách nguồn dữ liệu
            sources = self.list_sources()

            if not sources:
                logger.warning(
                    f"No sources found for compaction with format {self.file_format}"
                )
                return 0

            successful_compactions = 0

            for source_path in sources:
                logger.info(f"Checking source: {source_path}")

                # Tìm các nhóm file cần compact
                candidates = self.find_compaction_candidates(
                    source_path, min_days, max_days
                )

                if not candidates:
                    logger.info(f"No compaction candidates found for {source_path}")
                    continue

                # Compact từng nhóm file
                for directory_key, files in candidates.items():
                    logger.info(f"Compacting {len(files)} files for {directory_key}")

                    if self.compact_files(directory_key, files):
                        successful_compactions += 1

            logger.info(
                f"Compaction complete. Successfully compacted {successful_compactions} groups of files."
            )
            return successful_compactions

        except Exception as e:
            logger.error(f"Error running compaction: {e}")
            return 0


def main():
    parser = argparse.ArgumentParser(description="Run File Compaction Service")
    parser.add_argument(
        "--min-days", type=int, default=1, help="Minimum age of files to compact (days)"
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=None,
        help="Maximum age of files to compact (days)",
    )
    parser.add_argument(
        "--min-files", type=int, help="Minimum number of files to compact"
    )
    parser.add_argument("--target-size", type=int, help="Target file size in MB")
    parser.add_argument("--file-prefix", type=str, help="Prefix for the saved files")
    parser.add_argument(
        "--file-format", type=str, help="Format for saved files (parquet, csv, json)"
    )

    args = parser.parse_args()

    # Override environment variables with command line arguments
    if args.min_files:
        os.environ["MIN_COMPACT_FILES"] = str(args.min_files)
    if args.target_size:
        os.environ["TARGET_FILE_SIZE_MB"] = str(args.target_size)
    if args.file_prefix:
        os.environ["FILE_PREFIX"] = args.file_prefix
    if args.file_format:
        os.environ["FILE_FORMAT"] = args.file_format

    service = FileCompactionService()
    service.run_compaction(min_days=args.min_days, max_days=args.max_days)


if __name__ == "__main__":
    main()
