from abc import ABC, abstractmethod
from typing import List, Optional, Callable, Any, Dict, Awaitable
import asyncio
import random
from datetime import datetime



class BaseListCrawler(ABC):
    """
    Lớp cơ sở cho tất cả các list crawler
    """

    def __init__(self, source: str, max_concurrent: int = 5):
        self.source = source
        self.max_concurrent = max_concurrent
        self.running = True

        # Force crawl options
        self.force_crawl = False
        self.force_crawl_interval = 24.0  # Giờ
        self.should_crawl_page_callback = None

    def set_force_crawl_options(
        self,
        force_crawl: bool = False,
        force_crawl_interval: float = 24.0,
    ):
        """
        Thiết lập các tùy chọn force crawl

        Args:
            force_crawl: Có bật chế độ force crawl hay không
            force_crawl_interval: Khoảng thời gian (giờ) sau đó cần crawl lại
        """
        self.force_crawl = force_crawl
        self.force_crawl_interval = force_crawl_interval

    def set_should_crawl_page_callback(
        self, callback: Callable[[int], Awaitable[bool]]
    ):
        """
        Thiết lập callback để kiểm tra xem có nên crawl một trang hay không

        Args:
            callback: Hàm callback nhận page_number và trả về True/False
        """
        self.should_crawl_page_callback = callback

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
        self,
        items: List[Dict[str, Any]],
        callback: Optional[Callable] = None,
        page_number: Optional[int] = None,
    ) -> int:
        """
        Xử lý các items đã crawl được

        Args:
            items: Danh sách các items đã crawl được
            callback: Hàm callback để xử lý kết quả
            page_number: Số trang đang xử lý, để lưu checkpoint

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
            # Kiểm tra xem có nên crawl trang này hay không
            if self.should_crawl_page_callback:
                should_crawl = await self.should_crawl_page_callback(page_num)
                if not should_crawl:
                    print(
                        f"Skipping page {page_num} as it doesn't need to be recrawled yet"
                    )
                    return 0

            async with sem:
                try:
                    items = await self.crawl_page(page_num)
                    if items:
                        # Truyền page_number vào process_items để có thể lưu checkpoint
                        processed = await self.process_items(
                            items,
                            lambda data: callback(data, page_num) if callback else None,
                            page_num,
                        )
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
