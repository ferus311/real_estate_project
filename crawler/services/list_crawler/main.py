import os
import sys
import time
import signal
import logging
import argparse
import json
from datetime import datetime
from typing import Dict, Type, List, Any

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaProducer
from common.utils.logging_utils import setup_logging
from common.base.base_service import BaseService
from common.base.base_crawler import BaseCrawler
from common.factory.crawler_factory import CrawlerFactory
from sources.batdongsan.playwright.batdongsan_crawler import BatdongsanCrawler
from sources.chotot.api.chotot_api_crawler import ChototApiCrawler

# from sources.batdongsan.selenium.list_crawler import crawl_listings as selenium_crawl

from dotenv import load_dotenv

load_dotenv()

logger = setup_logging()

# Registry để đăng ký các crawler
CRAWLER_REGISTRY: Dict[str, Dict[str, Type[BaseCrawler]]] = {
    "batdongsan": {
        "playwright": BatdongsanCrawler,
        # Thêm các crawler khác cho batdongsan ở đây
    },
    "chotot": {
        "api": ChototApiCrawler,
        # Thêm các crawler khác cho chotot ở đây
    },
    # Thêm các source khác ở đây
}


class ListCrawlerService(BaseService):
    def __init__(self):
        super().__init__(service_name="List Crawler")
        self.source = os.environ.get("SOURCE", "batdongsan")
        self.crawler_type = os.environ.get("CRAWLER_TYPE", "playwright")
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "5"))
        self.start_page = int(os.environ.get("START_PAGE", "1"))
        self.end_page = int(os.environ.get("END_PAGE", "500"))
        self.output_file = os.environ.get("OUTPUT_FILE", None)
        self.producer = KafkaProducer() if not self.output_file else None
        self.collected_data = []  # Lưu dữ liệu khi xuất ra file

        # Xử lý tín hiệu để dừng service an toàn
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def get_crawler(self) -> BaseCrawler:
        """Lấy crawler instance dựa trên cấu hình"""
        try:
            return CrawlerFactory.create_list_crawler(
                source=self.source,
                crawler_type=self.crawler_type,
                max_concurrent=self.max_concurrent,
            )
        except ValueError as e:
            logger.error(f"Error creating crawler: {e}")
            raise

    def stop(self, *args):
        logger.info("Stopping List Crawler Service...")
        self.running = False

    async def crawl_and_publish(self):
        """Crawl danh sách và publish URL vào Kafka theo từng batch"""
        logger.info(f"Starting crawler for pages {self.start_page} to {self.end_page}")

        # Định nghĩa callback để xử lý URLs realtime
        async def kafka_publish_callback(url_batch):
            for url in url_batch:
                message = {
                    "url": url,
                    "source": self.source,
                    "timestamp": datetime.now().isoformat(),
                }
                if self.output_file:
                    self.collected_data.append(message)
                else:
                    self.producer.send("property-urls", message)

        try:
            crawler = self.get_crawler()
            url_count = await crawler.crawl_range(
                start_page=self.start_page,
                end_page=self.end_page,
                callback=kafka_publish_callback,
            )

            # Lưu dữ liệu vào file nếu cần
            if self.output_file and self.collected_data:
                self.save_to_file()

            logger.info(f"Crawled {url_count} URLs")
            if not self.output_file:
                logger.info(f"Published {url_count} URLs to Kafka")
            self.update_stats("successful", url_count)
            return url_count
        except Exception as e:
            logger.error(f"Error in crawler: {e}")
            self.update_stats("failed", 1)
            return 0

    def save_to_file(self) -> None:
        """Lưu dữ liệu đã thu thập vào file"""
        try:
            # Đảm bảo thư mục tồn tại
            os.makedirs(
                os.path.dirname(os.path.abspath(self.output_file)), exist_ok=True
            )

            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(self.collected_data, f, ensure_ascii=False, indent=2)

            logger.info(
                f"Đã lưu {len(self.collected_data)} bản ghi vào {self.output_file}"
            )
        except Exception as e:
            logger.error(f"Lỗi khi lưu file {self.output_file}: {e}")

    async def run_async(self):
        """Chạy service bất đồng bộ"""
        return await self.crawl_and_publish()

    def run(self, interval=3600):
        """Chạy service với chu kỳ lặp lại (mặc định là 1 giờ)"""
        logger.info(
            f"List Crawler Service started with {self.crawler_type} crawler for {self.source}"
        )

        try:
            while self.running:
                start_time = time.time()

                # Crawl và publish URLs
                import asyncio

                asyncio.run(self.run_async())

                # Tính thời gian cần sleep
                elapsed_time = time.time() - start_time
                sleep_time = max(0, interval - elapsed_time)

                if sleep_time > 0 and self.running:
                    logger.info(
                        f"Sleeping for {int(sleep_time)} seconds until next crawl cycle"
                    )
                    # Sleep với kiểm tra định kỳ để có thể dừng sớm
                    for _ in range(int(sleep_time)):
                        if not self.running:
                            break
                        time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping...")
        except Exception as e:
            logger.error(f"Error in List Crawler Service: {e}")
        finally:
            logger.info("List Crawler Service stopped")


def main():
    parser = argparse.ArgumentParser(description="Run List Crawler Service")
    parser.add_argument(
        "--interval", type=int, default=3600, help="Crawl interval in seconds"
    )
    parser.add_argument("--once", action="store_true", help="Crawl once and exit")
    parser.add_argument("--source", help="Source to crawl (batdongsan, chotot, etc.)")
    parser.add_argument(
        "--crawler-type", help="Type of crawler (playwright, api, etc.)"
    )
    parser.add_argument("--start-page", type=int, help="Start page number")
    parser.add_argument("--end-page", type=int, help="End page number")
    parser.add_argument("--output-file", help="Output file path for JSON data")

    args = parser.parse_args()

    # Cập nhật biến môi trường từ tham số dòng lệnh
    if args.source:
        os.environ["SOURCE"] = args.source
    if args.crawler_type:
        os.environ["CRAWLER_TYPE"] = args.crawler_type
    if args.start_page:
        os.environ["START_PAGE"] = str(args.start_page)
    if args.end_page:
        os.environ["END_PAGE"] = str(args.end_page)
    if args.output_file:
        os.environ["OUTPUT_FILE"] = args.output_file

    service = ListCrawlerService()

    if args.once:
        # Chạy một lần và thoát
        import asyncio

        asyncio.run(service.run_async())
    else:
        # Chạy liên tục với interval
        service.run(interval=args.interval)


if __name__ == "__main__":
    main()
