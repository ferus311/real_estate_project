import os
import sys
import time
import signal
import logging
from datetime import datetime

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaConsumer
from common.utils.logging_utils import setup_logging
from common.storage.hdfs_writer import HDFSWriter
from dotenv import load_dotenv

load_dotenv()

logger = setup_logging()


class StorageService:
    def __init__(self):
        self.running = True
        self.consumer = KafkaConsumer(
            ["property-data"], group_id="storage-service-group"
        )
        self.hdfs_writer = HDFSWriter()
        self.buffer = []
        self.buffer_size = int(os.environ.get("BUFFER_SIZE", "100"))
        self.flush_interval = int(os.environ.get("FLUSH_INTERVAL", "300"))  # 5 phút
        self.last_flush_time = time.time()

        # Xử lý tín hiệu để dừng service an toàn
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, *args):
        logger.info("Stopping Storage Service...")
        self.running = False
        self.flush_buffer()

    def process_message(self, message):
        """Xử lý một message từ Kafka và đưa vào buffer"""
        if not message:
            return False

        try:
            # Thêm message vào buffer
            self.buffer.append(message)

            # Kiểm tra nếu buffer đầy hoặc đã đến thời gian flush
            if (
                len(self.buffer) >= self.buffer_size
                or time.time() - self.last_flush_time >= self.flush_interval
            ):
                self.flush_buffer()

            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def flush_buffer(self):
        """Ghi buffer hiện tại vào HDFS và xóa buffer"""
        if not self.buffer:
            return

        try:
            # Tạo tên file dựa trên timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"property_data_{timestamp}.json"

            # Ghi dữ liệu vào HDFS
            logger.info(f"Flushing {len(self.buffer)} records to HDFS as {filename}")
            success = self.hdfs_writer.write_json(self.buffer, filename)

            if success:
                logger.info(f"Successfully wrote {len(self.buffer)} records to HDFS")
                # Xóa buffer sau khi ghi thành công
                self.buffer = []
                self.last_flush_time = time.time()
            else:
                logger.error(f"Failed to write data to HDFS")

        except Exception as e:
            logger.error(f"Error flushing buffer to HDFS: {e}")

    def run(self):
        """Chạy service liên tục"""
        logger.info("Storage Service started")

        try:
            while self.running:
                # Tiêu thụ message từ Kafka
                message = self.consumer.consume(timeout=1.0)

                if message:
                    self.process_message(message)
                else:
                    # Nếu không có message mới, kiểm tra xem có cần flush không
                    if (
                        time.time() - self.last_flush_time >= self.flush_interval
                        and self.buffer
                    ):
                        self.flush_buffer()
                    time.sleep(1)  # Tránh sử dụng CPU quá cao

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping...")
        except Exception as e:
            logger.error(f"Error in Storage Service: {e}")
        finally:
            # Đảm bảo dữ liệu còn lại được ghi vào HDFS
            if self.buffer:
                self.flush_buffer()

            self.consumer.close()
            logger.info("Storage Service stopped")


def main():
    service = StorageService()
    service.run()


if __name__ == "__main__":
    main()
