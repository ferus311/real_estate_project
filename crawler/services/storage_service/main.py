import os
import sys
import time
import signal
import logging
import json
import argparse
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
        self.batch_size = int(os.environ.get("BATCH_SIZE", "100"))
        self.flush_interval = int(os.environ.get("FLUSH_INTERVAL", "60"))  # seconds
        self.topic = os.environ.get("KAFKA_TOPIC", "property-data")
        self.group_id = os.environ.get("KAFKA_GROUP_ID", "storage-service-group")
        self.file_prefix = os.environ.get("FILE_PREFIX", "property_data")

        # Khởi tạo Kafka consumer
        self.consumer = KafkaConsumer([self.topic], group_id=self.group_id)

        # Khởi tạo storage
        self.storage = StorageFactory.create_storage(self.storage_type)

        # Buffer để tích lũy dữ liệu trước khi lưu
        self.data_buffer = []
        self.last_flush_time = time.time()

        logger.info(f"Storage Service initialized with {self.storage_type} storage")

    def should_flush(self) -> bool:
        """
        Kiểm tra xem có nên flush buffer không

        Returns:
            bool: True nếu nên flush, False nếu không
        """
        # Flush nếu buffer đủ lớn hoặc đã đủ thời gian
        return (
            len(self.data_buffer) >= self.batch_size
            or (time.time() - self.last_flush_time) >= self.flush_interval
        )

    def flush_buffer(self) -> bool:
        """
        Flush buffer vào storage

        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if not self.data_buffer:
            return True

        try:
            # Tạo tên file với timestamp
            file_path = self.storage.save_data(
                data=self.data_buffer, prefix=self.file_prefix
            )

            if file_path:
                logger.info(f"Saved {len(self.data_buffer)} records to {file_path}")
                self.update_stats("successful", len(self.data_buffer))
                self.data_buffer = []  # Xóa buffer sau khi lưu
                self.last_flush_time = time.time()
                return True
            else:
                logger.error("Failed to save data")
                self.update_stats("failed", len(self.data_buffer))
                return False

        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
            self.update_stats("failed", len(self.data_buffer))
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
            # Thêm vào buffer
            self.data_buffer.append(message)
            self.update_stats("processed")

            # Kiểm tra xem có nên flush không
            if self.should_flush():
                return self.flush_buffer()

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

        try:
            while self.running:
                # Consume message từ Kafka
                message = self.consumer.consume(timeout=1.0)

                if message:
                    self.process_message(message)
                else:
                    # Nếu không có message mới, kiểm tra xem có nên flush không
                    if self.should_flush() and self.data_buffer:
                        self.flush_buffer()

                    # Sleep một chút để không tiêu tốn CPU
                    time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in Storage Service: {e}")
        finally:
            # Flush buffer trước khi thoát
            if self.data_buffer:
                self.flush_buffer()

            self.consumer.close()
            self.report_stats()
            logger.info("Storage Service stopped")


def main():
    parser = argparse.ArgumentParser(description="Run Storage Service")
    parser.add_argument("--storage-type", type=str, help="Storage type (local, hdfs)")
    parser.add_argument("--batch-size", type=int, help="Batch size for storage")
    parser.add_argument("--flush-interval", type=int, help="Flush interval in seconds")

    args = parser.parse_args()

    # Override environment variables with command line arguments
    if args.storage_type:
        os.environ["STORAGE_TYPE"] = args.storage_type
    if args.batch_size:
        os.environ["BATCH_SIZE"] = str(args.batch_size)
    if args.flush_interval:
        os.environ["FLUSH_INTERVAL"] = str(args.flush_interval)

    service = StorageService()
    service.run()


if __name__ == "__main__":
    main()
