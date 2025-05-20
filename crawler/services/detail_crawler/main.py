import os
import sys
import time
import signal
import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Type
from collections import defaultdict

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaConsumer, KafkaProducer
from common.utils.logging_utils import setup_logging
from common.base.base_service import BaseService
from common.base.base_detail_crawler import BaseDetailCrawler
from common.factory.crawler_factory import CrawlerFactory

from dotenv import load_dotenv

load_dotenv()
logger = setup_logging()


class DetailCrawlerService(BaseService):
    def __init__(self):
        super().__init__(service_name="Detail Crawler")
        self.source = os.environ.get("SOURCE", "batdongsan")
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "5"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "100"))
        self.retry_limit = int(os.environ.get("RETRY_LIMIT", "3"))
        self.retry_delay = int(os.environ.get("RETRY_DELAY", "5"))
        self.crawler_type = os.environ.get("CRAWLER_TYPE", "default")
        self.output_topic = os.environ.get("OUTPUT_TOPIC", "property-data")

        # Kafka clients
        self.consumer = KafkaConsumer(
            ["property-urls"], group_id="detail-crawler-group"
        )
        self.producer = KafkaProducer()

        # Async components
        self.url_queue = asyncio.Queue(maxsize=100)
        self.batch_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # Tracking để commit offset
        self.processed_urls_count = 0
        self.commit_threshold = int(
            os.environ.get("COMMIT_THRESHOLD", "20")
        )  # Số URL xử lý trước khi commit
        self.last_commit_time = time.time()
        self.commit_interval = int(
            os.environ.get("COMMIT_INTERVAL", "60")
        )  # Thời gian giữa các lần commit (giây)
        self.commit_lock = asyncio.Lock()  # Lock để đảm bảo không commit đồng thời

        # Monitoring data
        self.stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "retries": 0,
            "start_time": time.time(),
            "commits": 0,  # Thêm thống kê số lần commit
        }
        self.failed_urls = defaultdict(int)

        # Khởi tạo crawler phù hợp
        self.crawler = self._get_crawler()

        # Signal handling
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def _get_crawler(self) -> BaseDetailCrawler:
        """Lấy crawler instance dựa trên cấu hình"""
        try:
            return CrawlerFactory.create_detail_crawler(
                source=self.source,
                crawler_type=self.crawler_type,
                max_concurrent=self.max_concurrent,
            )
        except ValueError as e:
            logger.error(f"Error creating crawler: {e}")
            raise

    def stop(self, *args):
        logger.info("Stopping Detail Crawler Service...")
        self.running = False
        self.report_stats()

    def report_stats(self):
        """Report crawling statistics"""
        runtime = time.time() - self.stats["start_time"]
        logger.info(f"=== Crawler Statistics ===")
        logger.info(f"Runtime: {runtime:.2f} seconds")
        logger.info(f"Processed: {self.stats['processed']} URLs")
        logger.info(f"Successful: {self.stats['successful']} URLs")
        logger.info(f"Failed: {self.stats['failed']} URLs")
        logger.info(f"Retries: {self.stats['retries']}")
        logger.info(f"Kafka Commits: {self.stats['commits']}")
        logger.info(f"Pending URLs since last commit: {self.processed_urls_count}")

        if self.stats["processed"] > 0:
            rate = self.stats["processed"] / runtime
            logger.info(f"Processing rate: {rate:.2f} URLs/second")
            success_rate = (self.stats["successful"] / self.stats["processed"]) * 100
            logger.info(f"Success rate: {success_rate:.2f}%")

    def update_stats(self, stat_key):
        """Cập nhật thống kê"""
        if stat_key in self.stats:
            self.stats[stat_key] += 1

    async def commit_offset_safely(self):
        """
        Commit offset một cách an toàn, sử dụng lock để tránh commit đồng thời từ nhiều worker
        """
        async with self.commit_lock:
            try:
                success = self.consumer.commit()
                if success:
                    self.stats["commits"] += 1
                    self.processed_urls_count = 0
                    self.last_commit_time = time.time()
                    logger.info(f"[Kafka] Successfully committed offset")
                else:
                    logger.warning(f"[Kafka] Failed to commit offset")
                return success
            except Exception as e:
                logger.error(f"[Kafka] Error committing offset: {e}")
                return False

    async def kafka_callback(self, result):
        """Send crawled data to Kafka"""
        try:
            # Gửi dữ liệu tới Kafka
            success = self.producer.send(self.output_topic, result)

            if success:
                logger.info(f"[Kafka] Sent data for: {result.get('url', 'unknown')}")
                self.update_stats("successful")

                # Tăng số lượng URL đã xử lý thành công
                self.processed_urls_count += 1

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

    async def kafka_consumer_loop(self):
        """Consume URLs from Kafka and add to queue"""
        logger.info("Starting Kafka consumer loop")
        batch = []

        while self.running:
            try:
                # Nếu url_queue đầy, đợi và không tiêu thụ Kafka
                if self.url_queue.full():
                    logger.warning("[Kafka] URL queue is full. Pausing consumption...")
                    await asyncio.sleep(1.0)
                    continue

                message = self.consumer.consume(timeout=1.0)
                if message and "url" in message:
                    batch.append(message)

                    # Khi đủ batch hoặc gần đầy, gửi qua batch queue
                    if len(batch) >= self.batch_size:
                        await self.batch_queue.put(batch.copy())
                        batch.clear()
                else:
                    if batch:
                        await self.batch_queue.put(batch.copy())
                        batch.clear()

                    # Kiểm tra nếu đã đủ thời gian từ lần commit trước đó
                    current_time = time.time()
                    if (
                        current_time - self.last_commit_time
                    ) >= self.commit_interval and self.processed_urls_count > 0:
                        logger.info(
                            f"[Kafka] Committing offset due to time interval ({self.commit_interval}s)"
                        )
                        await self.commit_offset_safely()

                    await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"[Kafka Consumer] Error: {e}")
                await asyncio.sleep(1.0)

        if batch:
            await self.batch_queue.put(batch)

    async def batch_processor_loop(self):
        """Process batches of URLs and distribute to worker queue"""
        logger.info("Starting batch processor loop")

        while self.running:
            try:
                batch = await asyncio.wait_for(self.batch_queue.get(), timeout=1.0)
                logger.info(f"Processing batch of {len(batch)} URLs")

                # Add each URL to the worker queue
                for message in batch:
                    await self.url_queue.put(message)

                self.batch_queue.task_done()
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"[Batch Processor] Error: {e}")
                await asyncio.sleep(0.5)

    async def worker_loop(self, worker_id: int):
        """Process URLs from the queue and crawl details"""
        logger.info(f"Starting worker {worker_id}")

        while self.running:
            try:
                message = await asyncio.wait_for(self.url_queue.get(), timeout=1.0)
                url = message["url"]
                metadata = {
                    "source": self.source,
                    "crawl_timestamp": int(datetime.now().timestamp()),
                    "url": url,
                }

                try:
                    async with self.semaphore:
                        self.update_stats("processed")
                        logger.info(f"[Worker {worker_id}] Processing: {url}")

                        # Sử dụng crawler phù hợp để crawl
                        detail_data = await self.crawler.crawl_detail(url)

                    if detail_data:
                        if detail_data.get("skipped"):
                            logger.info(f"[Worker {worker_id}] Skipped: {url}")
                        else:
                            # Thêm metadata và gửi đến Kafka
                            detail_data.update(metadata)
                            success = await self.kafka_callback(detail_data)

                            # Kiểm tra điều kiện để commit offset
                            current_time = time.time()
                            should_commit_by_count = (
                                self.processed_urls_count >= self.commit_threshold
                            )
                            should_commit_by_time = (
                                current_time - self.last_commit_time
                            ) >= self.commit_interval

                            if success and (
                                should_commit_by_count or should_commit_by_time
                            ):
                                logger.debug(
                                    f"[Worker {worker_id}] Committing offset after {self.processed_urls_count} URLs processed"
                                )
                                await self.commit_offset_safely()

                    else:
                        retry_count = self.failed_urls[url] + 1
                        self.failed_urls[url] = retry_count

                        if retry_count <= self.retry_limit:
                            logger.warning(
                                f"[Worker {worker_id}] Retry {retry_count}/{self.retry_limit} for: {url}"
                            )
                            self.update_stats("retries")
                            await asyncio.sleep(self.retry_delay)
                            await self.url_queue.put(message)
                        else:
                            logger.error(
                                f"[Worker {worker_id}] Max retries reached for: {url}"
                            )
                            self.update_stats("failed")

                except Exception as e:
                    logger.error(f"[Worker {worker_id}] Processing error: {e}")
                    self.update_stats("failed")

                self.url_queue.task_done()

            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Queue error: {e}")
                await asyncio.sleep(0.5)

    async def monitor_loop(self):
        """Regularly report statistics and monitor health"""
        logger.info("Starting monitoring loop")
        report_interval = 60  # Report every minute

        while self.running:
            await asyncio.sleep(report_interval)
            self.report_stats()

    async def run_async(self):
        logger.info(
            f"Service started with concurrency={self.max_concurrent}, batch_size={self.batch_size}, source={self.source}"
        )

        # Start all the required tasks
        consumer_task = asyncio.create_task(self.kafka_consumer_loop())
        batch_processor_task = asyncio.create_task(self.batch_processor_loop())
        monitor_task = asyncio.create_task(self.monitor_loop())

        # Start workers with unique IDs
        workers = [
            asyncio.create_task(self.worker_loop(i)) for i in range(self.max_concurrent)
        ]

        # Gather all tasks
        all_tasks = [consumer_task, batch_processor_task, monitor_task] + workers
        await asyncio.gather(*all_tasks)

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logger.info("Interrupted")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            # Thử commit offset một lần cuối trước khi đóng consumer
            try:
                self.consumer.commit()
                logger.info("Final offset committed before shutdown")
            except Exception as e:
                logger.error(f"Error during final commit: {e}")

            self.consumer.close()
            self.report_stats()
            logger.info("Service stopped")


def main():
    DetailCrawlerService().run()


if __name__ == "__main__":
    main()
