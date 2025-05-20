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
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "5"))
        self.start_page = int(os.environ.get("START_PAGE", "1"))
        self.end_page = int(os.environ.get("END_PAGE", "5"))
        self.region = os.environ.get("REGION", None)
        self.output_topic = os.environ.get("OUTPUT_TOPIC", "property-data")
        self.interval = int(os.environ.get("INTERVAL", "3600"))  # Mặc định: 1 giờ

        # Cấu hình dừng sớm khi gặp trang trống liên tiếp
        self.stop_on_empty = os.environ.get("STOP_ON_EMPTY", "true").lower() == "true"
        self.max_empty_pages = int(os.environ.get("MAX_EMPTY_PAGES", "2"))

        # Khởi tạo Kafka producer
        self.producer = KafkaProducer()

        # Trạng thái chạy
        self.running = True

        # Thống kê chi tiết hơn
        self.stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "pages_processed": 0,
            "total_pages_requested": 0,
            "empty_pages": 0,
            "early_stops": 0,
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
        """Báo cáo thống kê chi tiết hơn"""
        runtime = time.time() - self.stats["start_time"]
        logger.info(f"=== Crawler Statistics ===")
        logger.info(f"Runtime: {runtime:.2f} seconds")
        logger.info(f"Processed: {self.stats['processed']} items")
        logger.info(f"Successful: {self.stats['successful']} items")
        logger.info(f"Failed: {self.stats['failed']} items")
        logger.info(
            f"Pages Processed: {self.stats['pages_processed']} of {self.stats['total_pages_requested']} requested"
        )

        if self.stats.get("empty_pages", 0) > 0:
            logger.info(f"Empty pages encountered: {self.stats['empty_pages']}")

        if self.stats.get("early_stops", 0) > 0:
            logger.info(f"Early stops due to empty pages: {self.stats['early_stops']}")

        pages_saved = (
            self.stats["total_pages_requested"] - self.stats["pages_processed"]
        )
        if pages_saved > 0:
            logger.info(f"Pages saved by early stopping: {pages_saved}")

        if self.stats["processed"] > 0:
            rate = self.stats["processed"] / runtime
            logger.info(f"Processing rate: {rate:.2f} items/second")
            success_rate = (self.stats["successful"] / self.stats["processed"]) * 100
            logger.info(f"Success rate: {success_rate:.2f}%")

        if self.stats["pages_processed"] > 0:
            items_per_page = self.stats["processed"] / self.stats["pages_processed"]
            logger.info(f"Average items per page: {items_per_page:.2f}")

    def update_stats(self, stat_key, count=1):
        """Cập nhật thống kê"""
        if stat_key in self.stats:
            self.stats[stat_key] += count

    async def kafka_callback(self, result):
        """Gửi dữ liệu đã crawl đến Kafka"""
        max_retries = 3
        retry_count = 0
        retry_delay = 0.5  # Thời gian chờ giữa các lần retry (giây)

        while retry_count < max_retries:
            try:
                # Gửi dữ liệu tới Kafka
                metadata = {
                    "source": self.source,
                    "url": result.get("url", ""),
                    "crawl_timestamp": int(datetime.now().timestamp()),
                }

                result.update(metadata)

                success = self.producer.send(self.output_topic, result)

                if success:
                    logger.info(
                        f"[Kafka] Sent data for: {result.get('url', 'unknown')}"
                    )
                    self.update_stats("successful")
                    return True
                else:
                    retry_count += 1
                    logger.warning(
                        f"[Kafka] Failed to send data (attempt {retry_count}/{max_retries}): {result.get('url', 'unknown')}"
                    )
                    if retry_count < max_retries:
                        await asyncio.sleep(
                            retry_delay * retry_count
                        )  # Tăng thời gian chờ sau mỗi lần retry
            except Exception as e:
                retry_count += 1
                logger.error(
                    f"[Kafka] Error sending data (attempt {retry_count}/{max_retries}): {e}"
                )
                if retry_count < max_retries:
                    await asyncio.sleep(retry_delay * retry_count)

        # Nếu đã hết số lần retry mà vẫn thất bại
        self.update_stats("failed")
        return False

    async def crawl_once(self):
        """Chạy crawl một lần với khả năng dừng sớm khi gặp nhiều trang trống liên tiếp"""
        logger.info(
            f"Starting API crawl for {self.source} from page {self.start_page} to {self.end_page}"
        )
        logger.info(
            f"Stop on empty: {self.stop_on_empty}, Max empty pages: {self.max_empty_pages}"
        )

        try:
            # Lưu tổng số trang yêu cầu để thống kê hiệu quả của việc dừng sớm
            total_pages_requested = self.end_page - self.start_page + 1
            self.stats["total_pages_requested"] = total_pages_requested

            # Crawl range sử dụng callback để gửi từng item lên Kafka
            # Thêm tham số stop_on_empty và max_empty_pages để sử dụng cơ chế dừng sớm
            results = await self.crawler.crawl_range(
                start_page=self.start_page,
                end_page=self.end_page,
                region=self.region,
                callback=self.kafka_callback,
                stop_on_empty=self.stop_on_empty,
                max_empty_pages=self.max_empty_pages,
            )

            # Cập nhật thống kê
            self.update_stats("processed", results["total"])
            self.update_stats("pages_processed", results["pages_processed"])

            # Cập nhật thống kê về dừng sớm
            if results["pages_processed"] < total_pages_requested:
                early_stops = 1  # Đã dừng sớm ít nhất một lần
                self.stats["early_stops"] = early_stops
                self.stats["empty_pages"] = min(
                    self.max_empty_pages,
                    total_pages_requested - results["pages_processed"],
                )
                logger.info(
                    f"Crawler stopped early: processed {results['pages_processed']}/{total_pages_requested} pages"
                )

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
    parser.add_argument(
        "--stop-on-empty",
        type=str,
        choices=["true", "false"],
        help="Whether to stop crawling when empty pages are encountered",
    )
    parser.add_argument(
        "--max-empty-pages",
        type=int,
        help="Maximum consecutive empty pages before stopping",
    )

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
    if args.interval:
        os.environ["INTERVAL"] = str(args.interval)
    if args.stop_on_empty:
        os.environ["STOP_ON_EMPTY"] = args.stop_on_empty
    if args.max_empty_pages:
        os.environ["MAX_EMPTY_PAGES"] = str(args.max_empty_pages)

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
