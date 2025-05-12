from abc import ABC, abstractmethod
from typing import List, Optional, Callable, Any, Dict
import asyncio
import random
from datetime import datetime


class BaseCrawler(ABC):
    """
    Lớp cơ sở cho tất cả các crawler
    """

    def __init__(self, source: str, max_concurrent: int = 5):
        self.source = source
        self.max_concurrent = max_concurrent
        self.running = True
        self.checkpoint_file = f"./checkpoint/{source}_list_checkpoint.json"

    @abstractmethod
    async def crawl_page(self, page_number: int) -> List[Dict[str, Any]]:
        """
        Crawl một trang và trả về danh sách các items

        Args:
            page_number: Số trang cần crawl

        Returns:
            List[Dict]: Danh sách các items đã crawl được
        """
        pass

    @abstractmethod
    async def process_items(
        self, items: List[Dict[str, Any]], callback: Optional[Callable] = None
    ) -> int:
        """
        Xử lý các items đã crawl được

        Args:
            items: Danh sách các items đã crawl được
            callback: Hàm callback để xử lý kết quả

        Returns:
            int: Số lượng items đã xử lý thành công
        """
        pass

    async def crawl_range(
        self,
        start_page: int,
        end_page: int,
        callback: Optional[Callable] = None,
        force: bool = False,
    ) -> int:
        """
        Crawl một khoảng trang và xử lý kết quả

        Args:
            start_page: Trang bắt đầu
            end_page: Trang kết thúc
            callback: Hàm callback để xử lý kết quả
            force: Bỏ qua checkpoint

        Returns:
            int: Số lượng items đã xử lý
        """
        sem = asyncio.Semaphore(self.max_concurrent)
        total_processed = 0

        async def crawl_with_semaphore(page_num: int) -> int:
            async with sem:
                try:
                    items = await self.crawl_page(page_num)
                    if items:
                        processed = await self.process_items(items, callback)
                        return processed
                    return 0
                except Exception as e:
                    print(f"Error crawling page {page_num}: {e}")
                    return 0

        tasks = [crawl_with_semaphore(page) for page in range(start_page, end_page + 1)]
        results = await asyncio.gather(*tasks)

        return sum(results)

    def get_random_delay(
        self, min_seconds: float = 0.5, max_seconds: float = 2.0
    ) -> float:
        """
        Tạo thời gian delay ngẫu nhiên để tránh bị block

        Args:
            min_seconds: Thời gian tối thiểu (giây)
            max_seconds: Thời gian tối đa (giây)

        Returns:
            float: Thời gian delay (giây)
        """
        return random.uniform(min_seconds, max_seconds)

    def stop(self):
        """Dừng crawler"""
        self.running = False
