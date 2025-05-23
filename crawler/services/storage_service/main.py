import os
import sys
import time
import signal
import logging
import json
import argparse
import socket
import uuid
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
from common.utils.property_utils.classifier import get_property_type

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
        self.batch_size = int(os.environ.get("BATCH_SIZE", "20000"))
        self.flush_interval = int(os.environ.get("FLUSH_INTERVAL", "300"))  # 5 phút
        self.topic = os.environ.get("KAFKA_TOPIC", "property-data")
        self.group_id = os.environ.get("KAFKA_GROUP_ID", "storage-service-group")
        self.file_prefix = os.environ.get("FILE_PREFIX", "property_data")
        # Định dạng file mặc định là parquet, nhưng JSON được ưu tiên cho dữ liệu thô
        self.file_format = os.environ.get(
            "FILE_FORMAT", "json" if self.storage_type == "hdfs" else "parquet"
        )  # parquet, csv, json
        self.min_file_size_mb = int(os.environ.get("MIN_FILE_SIZE_MB", "5"))
        self.max_file_size_mb = int(os.environ.get("MAX_FILE_SIZE_MB", "128"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        self.retry_delay = int(os.environ.get("RETRY_DELAY", "1"))  # Giây

        # Cấu hình cho chế độ auto-stop (dừng tự động sau thời gian không hoạt động)
        # Khi bật run_once_mode, service sẽ tự động dừng sau idle_timeout giây không có message mới
        self.run_once_mode = os.environ.get("RUN_ONCE_MODE", "false").lower() == "true"
        # Thời gian chờ tối đa không có message trước khi dừng (mặc định: 1 giờ)
        self.idle_timeout = int(os.environ.get("IDLE_TIMEOUT", "3600"))
        # Theo dõi thời gian nhận message cuối cùng để phát hiện thời gian không hoạt động
        self.last_message_time = time.time()

        # Khởi tạo Kafka consumer
        self.consumer = KafkaConsumer([self.topic], group_id=self.group_id)

        # Khởi tạo storage
        self.storage = StorageFactory.create_storage(self.storage_type)

        # Buffer để tích lũy dữ liệu theo nguồn và loại bất động sản
        # Cấu trúc: {source: {property_type: [items]}}
        self.data_buffers = {}

        # Dict để theo dõi thời gian flush cuối cùng của mỗi nguồn và loại bất động sản
        # Cấu trúc: {source: {property_type: timestamp}}
        self.last_flush_time = {}

        # Các loại BĐS được hỗ trợ
        self.property_types = ["house", "apartment", "land", "commercial", "other"]

        logger.info(f"Storage Service initialized with {self.storage_type} storage")
        logger.info(
            f"Batch size: {self.batch_size}, Flush interval: {self.flush_interval}s, "
            f"File format: {self.file_format}, Min size: {self.min_file_size_mb}MB, Max size: {self.max_file_size_mb}MB"
        )
        if self.run_once_mode:
            logger.info(
                f"Running in ONCE mode: Will stop after {self.idle_timeout}s without messages"
            )

        # Set up signal handlers for graceful shutdown
        self._setup_signal_handler()

    def should_flush(self, source: str, property_type: str = None) -> bool:
        """
        Kiểm tra xem có nên flush buffer của nguồn cụ thể hoặc một loại bất động sản cụ thể không

        Args:
            source: Tên nguồn dữ liệu
            property_type: Loại bất động sản (nếu None, kiểm tra tổng hợp tất cả loại)

        Returns:
            bool: True nếu nên flush, False nếu không
        """
        if source not in self.data_buffers:
            return False

        current_time = time.time()

        # Nếu không chỉ định property_type, kiểm tra tổng hợp cho toàn bộ nguồn
        if property_type is None:
            total_count = 0
            # Đếm tổng số lượng bản ghi của tất cả các loại bất động sản
            for prop_type in self.property_types:
                if prop_type in self.data_buffers[source]:
                    total_count += len(self.data_buffers[source][prop_type])

            # Flush nếu tổng số lượng bản ghi đạt ngưỡng
            if total_count >= self.batch_size:
                return True

            # Kiểm tra thời gian flush cuối cùng của bất kỳ loại nào
            # Nếu có ít nhất một loại đã quá thời gian flush_interval, trả về True
            for prop_type in self.property_types:
                if prop_type in self.data_buffers[
                    source
                ] and prop_type in self.last_flush_time.get(source, {}):
                    last_flush = self.last_flush_time[source][prop_type]
                    if (current_time - last_flush) >= self.flush_interval:
                        return True

            # Ước tính kích thước tổng trong MB
            estimated_size_mb = total_count * 0.001
            if estimated_size_mb >= self.max_file_size_mb:
                return True

            return False
        else:
            # Kiểm tra cho một loại bất động sản cụ thể
            if property_type not in self.data_buffers.get(source, {}):
                return False

            buffer_count = len(self.data_buffers[source][property_type])

            # Lấy thời gian flush cuối cùng cho loại BĐS này
            last_flush = self.last_flush_time.get(source, {}).get(property_type, 0)

            # Ước tính kích thước trong MB cho loại BĐS này
            estimated_size_mb = buffer_count * 0.001

            # Kiểm tra các tiêu chí để flush
            return (
                buffer_count
                >= self.batch_size  # Sử dụng toàn bộ batch_size thay vì giảm ngưỡng
                or (current_time - last_flush) >= self.flush_interval
                or estimated_size_mb
                >= self.min_file_size_mb  # Sử dụng toàn bộ min_file_size_mb
                or estimated_size_mb >= self.max_file_size_mb
            )

    def flush_buffer(
        self, source: str, specific_property_type=None, retry_count=0
    ) -> bool:
        """
        Flush buffer của một nguồn và loại bất động sản cụ thể vào storage

        Args:
            source: Tên nguồn dữ liệu
            specific_property_type: Loại bất động sản cụ thể cần flush (nếu None, kiểm tra tất cả các loại)
            retry_count: Số lần đã thử lại (mặc định: 0)

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if source not in self.data_buffers:
            return True

        try:
            all_success = True
            processed_property_types = []  # Theo dõi các loại đã được lưu thành công
            processed_items_count = 0

            # Xác định các loại BĐS cần flush
            property_types_to_check = (
                [specific_property_type]
                if specific_property_type
                else self.property_types
            )

            for property_type in property_types_to_check:
                if property_type not in self.data_buffers.get(source, {}):
                    continue

                buffer = self.data_buffers[source][property_type]
                if not buffer:
                    continue

                # Kiểm tra xem loại BĐS này có đủ điều kiện để flush không
                should_flush_this_type = self.should_flush(source, property_type)

                # Nếu không đủ điều kiện flush và không phải đang retry
                if not should_flush_this_type and retry_count == 0:
                    logger.debug(
                        f"Skipping flush for {source}/{property_type}: not enough data yet"
                    )
                    continue

                # Tạo đường dẫn phân cấp theo nguồn và loại bất động sản
                now = datetime.now()
                date_str = now.strftime("%Y_%m_%d")
                time_str = now.strftime("%H%M%S")

                # Tạo thư mục phân cấp theo năm/tháng/ngày
                date_folder = now.strftime("%Y/%m/%d")

                # Xác định đường dẫn thư mục: raw/source/property_type/yyyy/mm/dd/
                relative_path = f"raw/{source}/{property_type}/{date_folder}"

                # Thêm hostname, microsecond và random UUID để đảm bảo tên file là duy nhất
                hostname = socket.gethostname()
                hostname_short = hostname.split(".")[0]
                # unique_id = uuid.uuid4().hex[:8]

                # Ưu tiên JSON cho dữ liệu thô
                save_format = (
                    "json" if self.storage_type == "hdfs" else self.file_format
                )
                file_name = f"{self.file_prefix}_{date_str}_{time_str}_{now.microsecond:06d}.{save_format}"

                # Tạo đường dẫn đầy đủ
                full_path = os.path.join(relative_path, file_name)

                # Lưu dữ liệu cho loại bất động sản này
                saved_path = self.storage.save_data(
                    data=buffer, file_name=full_path, file_format=save_format
                )

                if saved_path:
                    buffer_size_kb = len(buffer) * 1  # Ước tính mỗi bản ghi ~1KB
                    logger.info(
                        f"Saved {len(buffer)} records (~{buffer_size_kb/1024:.2f}MB) "
                        f"from {source}/{property_type} to {saved_path}"
                    )
                    self.update_stats("successful", len(buffer))
                    processed_items_count += len(buffer)
                    processed_property_types.append(property_type)

                    # Cập nhật thời gian flush cuối cùng
                    if property_type in self.data_buffers[source]:
                        # Xóa dữ liệu đã lưu thành công khỏi buffer
                        self.data_buffers[source][property_type] = []
                        self.last_flush_time[source][property_type] = time.time()
                else:
                    # Ghi log lỗi khi thất bại
                    logger.error(
                        f"Failed to save {len(buffer)} records for source {source}/{property_type}"
                    )
                    self.update_stats("failed", len(buffer))
                    all_success = False

            # Commit offset chỉ khi có bất kỳ dữ liệu nào được lưu thành công
            if processed_property_types:
                self.consumer.commit()
                logger.info(
                    f"Successfully processed and saved {processed_items_count} records from source {source} "
                    f"for property types: {', '.join(processed_property_types)}"
                )

            # Xử lý nếu có lỗi xảy ra
            if not all_success:
                # Thử lại nếu chưa đạt số lần thử tối đa
                if retry_count < self.max_retries:
                    retry_count += 1
                    retry_delay = self.retry_delay * retry_count  # Backoff tăng dần
                    logger.warning(
                        f"Lưu dữ liệu thất bại một phần, đang thử lại lần {retry_count}/{self.max_retries} sau {retry_delay} giây..."
                    )
                    time.sleep(retry_delay)
                    return self.flush_buffer(
                        source, specific_property_type, retry_count
                    )
                else:
                    # Nếu đã thử hết số lần mà vẫn thất bại
                    logger.error(
                        f"Failed to save some data from source {source} after {self.max_retries} attempts"
                    )
                    return False

            return True

        except Exception as e:
            # Thử lại trong trường hợp lỗi nếu chưa đạt số lần thử tối đa
            if retry_count < self.max_retries:
                retry_count += 1
                retry_delay = self.retry_delay * retry_count
                logger.warning(
                    f"Error flushing buffer for source {source}: {e}. Retrying ({retry_count}/{self.max_retries}) after {retry_delay}s..."
                )
                time.sleep(retry_delay)
                return self.flush_buffer(source, specific_property_type, retry_count)
            else:
                # Ghi log lỗi khi thất bại sau khi thử lại
                logger.error(
                    f"Error flushing buffer for source {source} after {self.max_retries} attempts: {e}"
                )
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

            # Thêm trường data_type nếu chưa có để phân loại
            if "data_type" not in message:
                message["data_type"] = get_property_type(message)

            # Xác định loại bất động sản
            property_type = get_property_type(message)

            # Khởi tạo buffer cho nguồn và loại bất động sản nếu chưa có
            if source not in self.data_buffers:
                self.data_buffers[source] = {}
                self.last_flush_time[source] = {}

            if property_type not in self.data_buffers[source]:
                self.data_buffers[source][property_type] = []
                self.last_flush_time[source][property_type] = time.time()

            # Thêm vào buffer tương ứng
            self.data_buffers[source][property_type].append(message)
            self.update_stats("processed")

            # Cập nhật thời gian nhận message
            self.last_message_time = time.time()

            # Kiểm tra xem có nên flush cho loại BĐS này không
            if self.should_flush(source, property_type):
                return self.flush_buffer(source, property_type)

            # Kiểm tra xem tổng hợp có nên flush không
            elif self.should_flush(source):
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
        if self.run_once_mode:
            logger.info(
                f"Storage Service started with {self.storage_type} storage in auto-stop mode (will stop after {self.idle_timeout} seconds without messages)"
            )
        else:
            logger.info(f"Storage Service started with {self.storage_type} storage")

        try:
            while self.running:
                # Consume message từ Kafka
                message = self.consumer.consume(timeout=1.0)

                if message:
                    self.process_message(message)
                else:
                    # Nếu không có message mới, kiểm tra các nguồn và loại BĐS có đủ điều kiện để flush không
                    for source in list(self.data_buffers.keys()):
                        for property_type in self.property_types:
                            if property_type in self.data_buffers.get(source, {}):
                                if self.should_flush(source, property_type):
                                    self.flush_buffer(source, property_type)

                        # Kiểm tra cả tổng hợp
                        if self.should_flush(source):
                            self.flush_buffer(source)

                    # Kiểm tra chế độ chạy một lần
                    if self.run_once_mode:
                        current_time = time.time()
                        # Nếu đã quá thời gian idle_timeout mà không có message mới, dừng service
                        if current_time - self.last_message_time > self.idle_timeout:
                            logger.info(
                                f"Auto-stop: No new messages received for {self.idle_timeout} seconds. Stopping service."
                            )

                            # Flush all remaining data before stopping
                            self._flush_all_buffers()
                            logger.info("All data buffers have been flushed.")

                            self.running = False

                    # Sleep một chút để không tiêu tốn CPU
                    time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully...")
            self.running = False
        except Exception as e:
            logger.error(f"Error in Storage Service: {e}")
        finally:
            # Flush all buffers before shutdown to ensure no data loss
            logger.info("Shutting down service, ensuring all data is saved...")
            self._flush_all_buffers()

            self.consumer.close()
            self.report_stats()
            logger.info("Storage Service stopped")

    def _flush_all_buffers(self):
        """
        Flush all data buffers for all sources and property types.
        This is used during shutdown to ensure all data is saved, regardless of buffer size or time criteria.

        Returns:
            dict: Dictionary with source and property type as keys and boolean success status as values
        """
        results = {}
        logger.info("Flushing all remaining data buffers before shutdown...")

        # Get a list of all sources with data in their buffers
        for source, property_types in self.data_buffers.items():
            source_results = {}

            for property_type, buffer in property_types.items():
                if not buffer:
                    continue

                logger.info(
                    f"Flushing buffer for {source}/{property_type} with {len(buffer)} records"
                )

                # Add force=True to bypass the usual flush criteria check
                # Initialize a timer to ensure unique filenames
                time.sleep(0.001)  # Ensure microsecond uniqueness

                # Unconditionally flush by forcing retry_count=1 which bypasses should_flush check
                success = self.flush_buffer(source, property_type, retry_count=1)
                source_results[property_type] = success

                if success:
                    logger.info(
                        f"Successfully flushed buffer for {source}/{property_type}"
                    )
                else:
                    logger.warning(
                        f"Failed to flush buffer for {source}/{property_type}"
                    )

            if source_results:
                results[source] = source_results

        if not results:
            logger.info("No data buffers to flush.")

        return results

    def _setup_signal_handler(self):
        """
        Set up signal handlers for graceful shutdown
        """

        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, shutting down gracefully...")
            self.running = False
            # Signal handlers should return quickly, so we don't flush buffers here
            # The main loop will handle flushing when self.running becomes False

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signal
        logger.debug("Signal handlers registered for graceful shutdown")


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
    # Auto-stop mode configuration
    parser.add_argument(
        "--once",
        action="store_true",
        help="Auto-stop mode: stop service after idle_timeout seconds without new messages",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=3600,
        help="Time in seconds to wait without messages before stopping in auto-stop mode (default: 3600)",
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
    if args.once:
        os.environ["RUN_ONCE_MODE"] = "true"
    # Since idle_timeout has a default value, we need to explicitly check if it was provided
    if args.idle_timeout != 3600 or "IDLE_TIMEOUT" not in os.environ:
        os.environ["IDLE_TIMEOUT"] = str(args.idle_timeout)

    service = StorageService()
    service.run()


if __name__ == "__main__":
    main()
