import os
import sys
import time
import signal
import asyncio
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Type
from collections import defaultdict

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaProducer
from common.utils.logging_utils import setup_logging
from common.base.base_service import BaseService
from common.base.base_api_crawler import BaseApiCrawler
from common.factory.crawler_factory import CrawlerFactory
from dotenv import load_dotenv

load_dotenv()
logger = setup_logging()


class ApiCrawlerService(BaseService):
    def __init__(self):
        super().__init__(service_name="API Crawler")
        self.source = os.environ.get("SOURCE", "chotot")
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "10"))
        self.start_page = int(os.environ.get("START_PAGE", "1"))
        self.end_page = int(os.environ.get("END_PAGE", "5"))
        self.category = os.environ.get("CATEGORY", "1000")  # Mặc định: BĐS
        self.region = os.environ.get("REGION", None)
        self.output_topic = os.environ.get("OUTPUT_TOPIC", "property-data")
        self.interval = int(os.environ.get("INTERVAL", "3600"))  # Mặc định: 1 giờ

        # Khởi tạo Kafka producer
        self.producer = KafkaProducer()

        # Trạng thái chạy
        self.running = True

        # Thống kê
        self.stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "start_time": time.time(),
        }

        # Khởi tạo crawler phù hợp
        self.crawler = self._get_crawler()

        # Signal handling
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def _get_crawler(self) -> BaseApiCrawler:
        """Lấy crawler instance dựa trên cấu hình"""
        try:
            return CrawlerFactory.create_api_crawler(
                source=self.source,
                max_concurrent=self.max_concurrent,
            )
        except ValueError as e:
            logger.error(f"Error creating crawler: {e}")
            raise

    def stop(self, *args):
        """Dừng service an toàn"""
        logger.info("Stopping API Crawler Service...")
        self.running = False
        self.report_stats()

    def report_stats(self):
        """Báo cáo thống kê"""
        runtime = time.time() - self.stats["start_time"]
        logger.info(f"=== Crawler Statistics ===")
        logger.info(f"Runtime: {runtime:.2f} seconds")
        logger.info(f"Processed: {self.stats['processed']} items")
        logger.info(f"Successful: {self.stats['successful']} items")
        logger.info(f"Failed: {self.stats['failed']} items")

        if self.stats["processed"] > 0:
            rate = self.stats["processed"] / runtime
            logger.info(f"Processing rate: {rate:.2f} items/second")
            success_rate = (self.stats["successful"] / self.stats["processed"]) * 100
            logger.info(f"Success rate: {success_rate:.2f}%")

    def update_stats(self, stat_key, count=1):
        """Cập nhật thống kê"""
        if stat_key in self.stats:
            self.stats[stat_key] += count

    async def kafka_callback(self, result):
        """Gửi dữ liệu đã crawl đến Kafka"""
        try:
            # Đảm bảo result có timestamp
            result.setdefault("timestamp", datetime.now().isoformat())

            # Đảm bảo các trường quan trọng tồn tại cho model HouseDetailItem
            if "price" not in result or not result["price"]:
                result["price"] = "0"

            if "area" not in result or not result["area"]:
                result["area"] = "0"

            # Đảm bảo seller_info là dictionary
            if "seller_info" not in result or not isinstance(
                result["seller_info"], dict
            ):
                result["seller_info"] = {}

            # Thêm các trường cần thiết cho mô hình dữ liệu nếu chúng không tồn tại
            result.setdefault("listing_id", "")
            result.setdefault("url", "")

            # Gửi dữ liệu tới Kafka
            success = self.producer.send(self.output_topic, result)

            if success:
                logger.info(f"[Kafka] Sent data for: {result.get('url', 'unknown')}")
                self.update_stats("successful")
                return True
            else:
                logger.error(
                    f"[Kafka] Failed to send data: {result.get('url', 'unknown')}"
                )
                self.update_stats("failed")
                return False

        except Exception as e:
            logger.error(f"[Kafka] Error sending data: {e}")
            self.update_stats("failed")
            return False

    async def crawl_once(self):
        """Chạy crawl một lần"""
        logger.info(
            f"Starting API crawl for {self.source} from page {self.start_page} to {self.end_page}"
        )

        try:
            # Crawl range sử dụng callback để gửi từng item lên Kafka
            results = await self.crawler.crawl_range(
                start_page=self.start_page,
                end_page=self.end_page,
                region=self.region,
                category=self.category,
                callback=self.kafka_callback,
            )

            # Cập nhật thống kê
            self.update_stats("processed", results["total"])

            logger.info(
                f"API Crawler completed: {results['successful']} successful, {results['failed']} failed"
            )
            return results

        except Exception as e:
            logger.error(f"Error in API crawler: {e}")
            return None

    async def run_async(self):
        """Chạy service bất đồng bộ"""
        return await self.crawl_once()

    def run(self, interval=None):
        """Chạy service với chu kỳ lặp lại"""
        if interval is None:
            interval = self.interval

        logger.info(f"Starting API Crawler Service with interval {interval}s")

        try:
            while self.running:
                start_time = time.time()

                # Chạy một lần crawl
                asyncio.run(self.crawl_once())

                # Report stats
                self.report_stats()

                # Nếu đã bị dừng sau khi crawl, thoát luôn
                if not self.running:
                    break

                # Tính thời gian cần sleep
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)

                if sleep_time > 0:
                    logger.info(f"Sleeping for {sleep_time:.2f}s before next crawl...")

                    # Sleep với kiểm tra định kỳ xem có bị stop không
                    sleep_chunk = 5  # Kiểm tra mỗi 5 giây
                    for _ in range(0, int(sleep_time), sleep_chunk):
                        if not self.running:
                            break
                        time.sleep(min(sleep_chunk, sleep_time))

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping...")
        except Exception as e:
            logger.error(f"Error in API Crawler Service: {e}")
        finally:
            logger.info("API Crawler Service stopped")


def main():
    parser = argparse.ArgumentParser(description="Run API Crawler Service")
    parser.add_argument("--interval", type=int, help="Crawl interval in seconds")
    parser.add_argument("--once", action="store_true", help="Crawl once and exit")
    parser.add_argument("--source", help="Source to crawl (chotot, etc.)")
    parser.add_argument("--start-page", type=int, help="Start page number")
    parser.add_argument("--end-page", type=int, help="End page number")
    parser.add_argument("--region", help="Region ID for filtering")
    parser.add_argument("--category", help="Category ID for filtering")

    args = parser.parse_args()

    # Cập nhật biến môi trường từ tham số dòng lệnh
    if args.source:
        os.environ["SOURCE"] = args.source
    if args.start_page:
        os.environ["START_PAGE"] = str(args.start_page)
    if args.end_page:
        os.environ["END_PAGE"] = str(args.end_page)
    if args.region:
        os.environ["REGION"] = args.region
    if args.category:
        os.environ["CATEGORY"] = args.category
    if args.interval:
        os.environ["INTERVAL"] = str(args.interval)

    service = ApiCrawlerService()

    if args.once:
        # Chạy một lần và thoát
        import asyncio

        asyncio.run(service.run_async())
    else:
        # Chạy liên tục với interval
        service.run(interval=args.interval)


if __name__ == "__main__":
    main()
