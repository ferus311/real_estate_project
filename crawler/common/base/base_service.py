from abc import ABC, abstractmethod
import os
import sys
import time
import signal
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime


class BaseService(ABC):
    """
    Lớp cơ sở cho tất cả các service trong hệ thống crawler
    """

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.running = True
        self.start_time = time.time()

        # Cấu hình chung từ biến môi trường
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "5"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "100"))
        self.retry_limit = int(os.environ.get("RETRY_LIMIT", "3"))
        self.retry_delay = int(os.environ.get("RETRY_DELAY", "5"))

        # Thống kê
        self.stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "retries": 0,
            "start_time": self.start_time,
        }

        # Xử lý tín hiệu
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        logging.info(f"{self.service_name} Service initialized")

    def stop(self, *args):
        """Dừng service an toàn"""
        logging.info(f"Stopping {self.service_name} Service...")
        self.running = False
        self.report_stats()

    def report_stats(self):
        """Báo cáo thống kê hoạt động"""
        runtime = time.time() - self.stats["start_time"]
        logging.info(f"=== {self.service_name} Statistics ===")
        logging.info(f"Runtime: {runtime:.2f} seconds")
        logging.info(f"Processed: {self.stats['processed']}")
        logging.info(f"Successful: {self.stats['successful']}")
        logging.info(f"Failed: {self.stats['failed']}")
        logging.info(f"Retries: {self.stats['retries']}")

        if self.stats["processed"] > 0:
            rate = self.stats["processed"] / runtime
            logging.info(f"Processing rate: {rate:.2f} items/second")
            success_rate = (self.stats["successful"] / self.stats["processed"]) * 100
            logging.info(f"Success rate: {success_rate:.2f}%")

    @abstractmethod
    def run(self):
        """Chạy service"""
        pass

    @abstractmethod
    async def run_async(self):
        """Chạy service bất đồng bộ"""
        pass

    def update_stats(self, key: str, increment: int = 1):
        """Cập nhật thống kê"""
        if key in self.stats:
            self.stats[key] += increment
