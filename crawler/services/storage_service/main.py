import os
import sys
import time
import signal
import logging
import json
import argparse
import socket
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaConsumer, KafkaProducer
from common.utils.logging_utils import setup_logging
from common.base.base_service import BaseService
from common.base.base_storage import BaseStorage
from common.factory.storage_factory import StorageFactory

from dotenv import load_dotenv

load_dotenv()
logger = setup_logging()


class StorageService(BaseService):
    """
    Service quản lý việc lưu trữ dữ liệu từ Kafka vào các storage
    """

    def __init__(self):
        super().__init__(service_name="Storage Service")

        # Cấu hình từ biến môi trường
        self.storage_type = os.environ.get("STORAGE_TYPE", "local")
        self.batch_size = int(os.environ.get("BATCH_SIZE", "10000"))
        self.flush_interval = int(os.environ.get("FLUSH_INTERVAL", "300"))  # 5 phút
        self.topic = os.environ.get("KAFKA_TOPIC", "property-data")
        self.group_id = os.environ.get("KAFKA_GROUP_ID", "storage-service-group")
        self.file_prefix = os.environ.get("FILE_PREFIX", "property_data")
        self.file_format = os.environ.get(
            "FILE_FORMAT", "parquet"
        )  # parquet, csv, json
        self.min_file_size_mb = int(os.environ.get("MIN_FILE_SIZE_MB", "2"))
        self.max_file_size_mb = int(os.environ.get("MAX_FILE_SIZE_MB", "128"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        self.retry_delay = int(os.environ.get("RETRY_DELAY", "1"))  # Giây
        self.process_backup_on_startup = (
            os.environ.get("PROCESS_BACKUP_ON_STARTUP", "true").lower() == "true"
        )
        self.backup_dir = os.environ.get("BACKUP_DIR", "backup_data")

        # Khởi tạo Kafka consumer
        self.consumer = KafkaConsumer([self.topic], group_id=self.group_id)

        # Khởi tạo storage
        self.storage = StorageFactory.create_storage(self.storage_type)

        # Buffer để tích lũy dữ liệu theo nguồn
        self.data_buffers = {}  # Dict với key là nguồn
        self.buffer_sizes = {}  # Dict để theo dõi kích thước của mỗi buffer
        self.last_flush_time = (
            {}
        )  # Dict để theo dõi thời gian flush cuối cùng của mỗi nguồn

        # Đảm bảo thư mục backup tồn tại
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)

        logger.info(f"Storage Service initialized with {self.storage_type} storage")
        logger.info(
            f"Batch size: {self.batch_size}, Flush interval: {self.flush_interval}s, "
            f"File format: {self.file_format}, Min size: {self.min_file_size_mb}MB, Max size: {self.max_file_size_mb}MB"
        )

    def should_flush(self, source: str) -> bool:
        """
        Kiểm tra xem có nên flush buffer của nguồn cụ thể không

        Args:
            source: Tên nguồn dữ liệu

        Returns:
            bool: True nếu nên flush, False nếu không
        """
        current_time = time.time()
        last_flush = self.last_flush_time.get(source, 0)
        buffer_size = len(self.data_buffers.get(source, []))

        # Ước tính kích thước buffer trong MB (trung bình 1 bản ghi ~ 1KB)
        estimated_size_mb = buffer_size * 0.001

        # Flush nếu:
        # 1. Buffer đủ lớn theo số lượng bản ghi
        # 2. Đã đủ thời gian từ lần flush cuối
        # 3. Ước tính kích thước đạt mức tối thiểu
        # 4. Ước tính kích thước vượt quá mức tối đa
        return (
            buffer_size >= self.batch_size
            or (current_time - last_flush) >= self.flush_interval
            or estimated_size_mb >= self.min_file_size_mb
            or estimated_size_mb >= self.max_file_size_mb
        )

    def _save_to_backup(self, source: str, data: List[Dict[str, Any]]) -> bool:
        """
        Lưu dữ liệu vào file backup để xử lý lại sau

        Args:
            source: Nguồn dữ liệu
            data: Dữ liệu cần lưu

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            if not os.path.exists(self.backup_dir):
                os.makedirs(self.backup_dir)

            # Tạo tên file backup với timestamp để tránh trùng lặp
            hostname = socket.gethostname()
            now = datetime.now()
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            backup_file = f"{self.backup_dir}/{source}_{timestamp}_{hostname}.json"

            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            logger.info(
                f"Đã lưu {len(data)} bản ghi từ nguồn {source} vào file backup: {backup_file}"
            )
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu backup cho nguồn {source}: {e}")
            return False

    def _process_backup_files(self):
        """Xử lý các file backup từ lần chạy trước"""
        if not os.path.exists(self.backup_dir):
            logger.info("Không có thư mục backup để xử lý")
            return

        backup_files = [f for f in os.listdir(self.backup_dir) if f.endswith(".json")]
        if not backup_files:
            logger.info("Không tìm thấy file backup nào")
            return

        logger.info(f"Tìm thấy {len(backup_files)} file backup cần xử lý")

        for file in backup_files:
            try:
                file_path = os.path.join(self.backup_dir, file)
                # Xác định nguồn từ tên file (phần đầu tiên trước dấu _)
                source = file.split("_")[0]

                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                logger.info(f"Đang xử lý file backup: {file} với {len(data)} bản ghi")

                # Thêm dữ liệu vào buffer hiện tại
                if source not in self.data_buffers:
                    self.data_buffers[source] = []
                    self.buffer_sizes[source] = 0
                    self.last_flush_time[source] = time.time()

                self.data_buffers[source].extend(data)
                self.buffer_sizes[source] = len(self.data_buffers[source])

                # Flush buffer ngay lập tức
                if self.flush_buffer(source):
                    # Xóa file backup nếu lưu thành công
                    os.remove(file_path)
                    logger.info(f"File backup {file} đã được xử lý và xóa")
                else:
                    logger.warning(
                        f"Không thể xử lý file backup {file}, sẽ thử lại sau"
                    )
            except Exception as e:
                logger.error(f"Lỗi khi xử lý file backup {file}: {e}")

    def flush_buffer(self, source: str, retry_count=0) -> bool:
        """
        Flush buffer của một nguồn cụ thể vào storage

        Args:
            source: Tên nguồn dữ liệu
            retry_count: Số lần đã thử lại (mặc định: 0)

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        buffer = self.data_buffers.get(source, [])
        if not buffer:
            return True

        try:
            # Tạo đường dẫn chỉ phân cấp theo nguồn
            relative_path = f"{source}"

            # Tạo tên file có định dạng ngày tháng rõ ràng
            now = datetime.now()
            date_str = now.strftime("%Y_%m_%d")
            time_str = now.strftime("%H%M%S")

            # Thêm hostname vào tên file để tránh xung đột khi nhiều service cùng chạy
            hostname = socket.gethostname()
            hostname_short = hostname.split(".")[0]  # Lấy phần đầu tiên của hostname

            file_name = f"{self.file_prefix}_{date_str}_{time_str}_{hostname_short}.{self.file_format}"

            # Tạo đường dẫn đầy đủ
            full_path = os.path.join(relative_path, file_name)

            # Lưu dữ liệu
            saved_path = self.storage.save_data(
                data=buffer, file_name=full_path, file_format=self.file_format
            )

            if saved_path:
                buffer_size_kb = len(buffer) * 1  # Ước tính mỗi bản ghi ~1KB
                logger.info(
                    f"Saved {len(buffer)} records (~{buffer_size_kb/1024:.2f}MB) from {source} to {saved_path}"
                )
                self.update_stats("successful", len(buffer))
                self.data_buffers[source] = []  # Xóa buffer sau khi lưu
                self.buffer_sizes[source] = 0
                self.last_flush_time[source] = time.time()

                # Commit offset chỉ khi lưu thành công
                self.consumer.commit()
                return True
            else:
                # Thử lại nếu chưa đạt số lần thử tối đa
                if retry_count < self.max_retries:
                    retry_count += 1
                    retry_delay = self.retry_delay * retry_count  # Backoff tăng dần
                    logger.warning(
                        f"Lưu dữ liệu thất bại, đang thử lại lần {retry_count}/{self.max_retries} sau {retry_delay} giây..."
                    )
                    time.sleep(retry_delay)
                    return self.flush_buffer(source, retry_count)
                else:
                    # Lưu vào backup nếu đã thử hết số lần
                    logger.error(
                        f"Failed to save data from source {source} after {self.max_retries} attempts"
                    )
                    self._save_to_backup(source, buffer)
                    self.update_stats("failed", len(buffer))
                    return False

        except Exception as e:
            # Thử lại trong trường hợp lỗi nếu chưa đạt số lần thử tối đa
            if retry_count < self.max_retries:
                retry_count += 1
                retry_delay = self.retry_delay * retry_count
                logger.warning(
                    f"Error flushing buffer for source {source}: {e}. Retrying ({retry_count}/{self.max_retries}) after {retry_delay}s..."
                )
                time.sleep(retry_delay)
                return self.flush_buffer(source, retry_count)
            else:
                # Lưu vào backup nếu đã thử hết số lần
                logger.error(
                    f"Error flushing buffer for source {source} after {self.max_retries} attempts: {e}"
                )
                self._save_to_backup(source, buffer)
                self.update_stats("failed", len(buffer))
                return False

    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Xử lý một message từ Kafka

        Args:
            message: Message từ Kafka

        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        try:
            # Xác định nguồn dữ liệu từ message
            source = message.get("source", "unknown")

            # Khởi tạo buffer cho nguồn nếu chưa có
            if source not in self.data_buffers:
                self.data_buffers[source] = []
                self.buffer_sizes[source] = 0
                self.last_flush_time[source] = time.time()

            # Thêm vào buffer tương ứng
            self.data_buffers[source].append(message)
            self.buffer_sizes[source] = len(self.data_buffers[source])
            self.update_stats("processed")

            # Kiểm tra xem có nên flush không
            if self.should_flush(source):
                return self.flush_buffer(source)

            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.update_stats("failed")
            return False

    async def run_async(self):
        """
        Chạy service bất đồng bộ
        """
        # Storage service không cần async
        pass

    def run(self):
        """
        Chạy service
        """
        logger.info(f"Storage Service started with {self.storage_type} storage")

        # Xử lý các file backup trước khi bắt đầu tiêu thụ dữ liệu mới
        if self.process_backup_on_startup:
            self._process_backup_files()

        try:
            while self.running:
                # Consume message từ Kafka
                message = self.consumer.consume(timeout=1.0)

                if message:
                    self.process_message(message)
                else:
                    # Nếu không có message mới, kiểm tra xem có nguồn nào cần flush không
                    for source in list(self.data_buffers.keys()):
                        if self.should_flush(source) and self.data_buffers[source]:
                            self.flush_buffer(source)

                    # Sleep một chút để không tiêu tốn CPU
                    time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in Storage Service: {e}")
        finally:
            # Flush tất cả các buffer trước khi thoát
            for source in list(self.data_buffers.keys()):
                if self.data_buffers[source]:
                    # Nếu flush không thành công, lưu vào backup
                    if not self.flush_buffer(source):
                        self._save_to_backup(source, self.data_buffers[source])
                        logger.info(
                            f"Đã lưu dữ liệu của nguồn {source} vào backup trước khi thoát"
                        )

            self.consumer.close()
            self.report_stats()
            logger.info("Storage Service stopped")


def main():
    parser = argparse.ArgumentParser(description="Run Storage Service")
    parser.add_argument("--storage-type", type=str, help="Storage type (local, hdfs)")
    parser.add_argument("--batch-size", type=int, help="Batch size for storage")
    parser.add_argument("--flush-interval", type=int, help="Flush interval in seconds")
    parser.add_argument("--file-prefix", type=str, help="Prefix for the saved files")
    parser.add_argument(
        "--file-format", type=str, help="Format for saved files (parquet, csv, json)"
    )
    parser.add_argument("--min-file-size", type=int, help="Minimum file size in MB")
    parser.add_argument("--max-file-size", type=int, help="Maximum file size in MB")
    parser.add_argument(
        "--max-retries", type=int, help="Maximum retry attempts when saving fails"
    )
    parser.add_argument(
        "--retry-delay", type=int, help="Base delay between retries in seconds"
    )
    parser.add_argument(
        "--process-backup",
        type=str,
        help="Process backup files on startup (true/false)",
    )
    parser.add_argument(
        "--backup-dir", type=str, help="Directory to store backup files"
    )

    args = parser.parse_args()

    # Override environment variables with command line arguments
    if args.storage_type:
        os.environ["STORAGE_TYPE"] = args.storage_type
    if args.batch_size:
        os.environ["BATCH_SIZE"] = str(args.batch_size)
    if args.flush_interval:
        os.environ["FLUSH_INTERVAL"] = str(args.flush_interval)
    if args.file_prefix:
        os.environ["FILE_PREFIX"] = args.file_prefix
    if args.file_format:
        os.environ["FILE_FORMAT"] = args.file_format
    if args.min_file_size:
        os.environ["MIN_FILE_SIZE_MB"] = str(args.min_file_size)
    if args.max_file_size:
        os.environ["MAX_FILE_SIZE_MB"] = str(args.max_file_size)
    if args.max_retries:
        os.environ["MAX_RETRIES"] = str(args.max_retries)
    if args.retry_delay:
        os.environ["RETRY_DELAY"] = str(args.retry_delay)
    if args.process_backup:
        os.environ["PROCESS_BACKUP_ON_STARTUP"] = args.process_backup
    if args.backup_dir:
        os.environ["BACKUP_DIR"] = args.backup_dir

    service = StorageService()
    service.run()


if __name__ == "__main__":
    main()
