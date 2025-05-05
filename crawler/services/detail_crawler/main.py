import os
import sys
import time
import signal
import asyncio
import logging
from datetime import datetime

# Thêm thư mục gốc vào sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from common.queue.kafka_client import KafkaConsumer, KafkaProducer
from common.utils.logging_utils import setup_logging
from sources.batdongsan.playwright.detail_crawler import crawl_detail
from dotenv import load_dotenv

load_dotenv()

logger = setup_logging()


class DetailCrawlerService:
    def __init__(self):
        self.running = True
        self.source = os.environ.get("SOURCE", "batdongsan")
        self.max_concurrent = int(os.environ.get("MAX_CONCURRENT", "5"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "10"))
        self.consumer = KafkaConsumer(
            ["property-urls"], group_id="detail-crawler-group"
        )
        self.producer = KafkaProducer()
        self.semaphore = None  # Sẽ được khởi tạo trong event loop

        # Xử lý tín hiệu để dừng service an toàn
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, *args):
        logger.info("Stopping Detail Crawler Service...")
        self.running = False

    async def process_url(self, url, metadata=None):
        """Thu thập chi tiết từ một URL và gửi lên Kafka"""
        try:
            logger.info(f"Processing URL: {url}")

            # Sử dụng semaphore để giới hạn số lượng crawler đồng thời
            async with self.semaphore:
                detail_data = await crawl_detail(url)

            if not detail_data:
                logger.warning(f"No data extracted from URL: {url}")
                return False

            # Bổ sung metadata
            if metadata:
                detail_data.update(metadata)

            # Thêm timestamp nếu chưa có
            if "timestamp" not in detail_data:
                detail_data["timestamp"] = datetime.now().isoformat()

            # Gửi lên Kafka
            success = self.producer.send("property-data", detail_data)

            if success:
                logger.info(f"Successfully sent detail data to Kafka for URL: {url}")
                return True
            else:
                logger.error(f"Failed to send detail data to Kafka for URL: {url}")
                return False

        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            return False

    async def consume_messages(self):
        """Tiêu thụ messages từ Kafka và đưa vào queue để xử lý"""
        queue = asyncio.Queue()

        # Task tiêu thụ từ Kafka
        async def kafka_consumer_task():
            while self.running:
                message = self.consumer.consume(timeout=1.0)
                if message and "url" in message:
                    url = message["url"]
                    # Loại bỏ "url" từ metadata để tránh trùng lặp
                    metadata = {k: v for k, v in message.items() if k != "url"}
                    await queue.put((url, metadata))
                else:
                    await asyncio.sleep(0.1)  # Tránh CPU cao khi không có message

        # Task xử lý URLs
        async def url_processor_task():
            while self.running:
                try:
                    # Lấy nhiều items để xử lý song song
                    tasks = []
                    for _ in range(min(self.batch_size, queue.qsize() + 1)):
                        try:
                            url, metadata = await asyncio.wait_for(
                                queue.get(), timeout=0.5
                            )
                            task = asyncio.create_task(self.process_url(url, metadata))
                            tasks.append(task)
                            # Đánh dấu task hoàn thành khi xong
                            task.add_done_callback(lambda _: queue.task_done())
                        except asyncio.TimeoutError:
                            break  # Không có message nào trong queue

                    if tasks:
                        # Đợi tất cả tasks hoàn thành
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        success_count = sum(1 for r in results if r is True)
                        logger.info(
                            f"Processed {success_count}/{len(tasks)} URLs successfully"
                        )
                    else:
                        await asyncio.sleep(0.5)  # Nghỉ khi không có task nào

                except Exception as e:
                    logger.error(f"Error in url_processor_task: {e}")
                    await asyncio.sleep(1)

        # Khởi chạy consumer task
        consumer_task = asyncio.create_task(kafka_consumer_task())
        # Khởi chạy processor task
        processor_task = asyncio.create_task(url_processor_task())

        # Đợi cả hai tasks kết thúc
        await asyncio.gather(consumer_task, processor_task)

    async def run_async(self):
        """Chạy service trong event loop"""
        logger.info(
            f"Detail Crawler Service started for {self.source} with concurrency {self.max_concurrent}"
        )

        # Khởi tạo semaphore để giới hạn số lượng crawler đồng thời
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        try:
            await self.consume_messages()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.consumer.close()
            logger.info("Detail Crawler Service stopped")

    def run(self):
        """Chạy service liên tục"""
        try:
            # Khởi tạo và chạy event loop
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping...")
        except Exception as e:
            logger.error(f"Error in Detail Crawler Service: {e}")


def main():
    service = DetailCrawlerService()
    service.run()


if __name__ == "__main__":
    main()
