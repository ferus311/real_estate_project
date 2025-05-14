from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Callable, Union
import asyncio
import aiohttp
import random
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseApiCrawler(ABC):
    """
    Lớp cơ sở cho tất cả các API crawler
    """

    def __init__(self, source: str, max_concurrent: int = 10):
        self.source = source
        self.max_concurrent = max_concurrent
        self.running = True
        self.checkpoint_file = f"./checkpoint/{source}_api_checkpoint.json"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.max_retries = 3

    def get_random_delay(self, min_delay: int = 1, max_delay: int = 3) -> float:
        """
        Tạo độ trễ ngẫu nhiên giữa các lần gọi API

        Args:
            min_delay: Thời gian tối thiểu (giây)
            max_delay: Thời gian tối đa (giây)

        Returns:
            float: Độ trễ ngẫu nhiên
        """
        return random.uniform(min_delay, max_delay)

    async def fetch_api(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[Dict[str, Any]]:
        """
        Gọi API và lấy dữ liệu

        Args:
            session: aiohttp session
            url: URL API

        Returns:
            Dict: Dữ liệu API, hoặc None nếu có lỗi
        """
        retries = self.max_retries

        while retries > 0:
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = self.get_random_delay(2, 5)
                        logger.warning(f"Rate limited. Waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        retries -= 1
                    else:
                        logger.error(f"API error: {response.status} for {url}")
                        retries -= 1
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Request error: {e} for {url}")
                retries -= 1
                await asyncio.sleep(1)

        return None

    @abstractmethod
    async def crawl_listings(
        self,
        page: int = 1,
        limit: int = 50,
        callback: Optional[Callable] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Crawl danh sách các listings từ một trang

        Args:
            page: Số trang
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả
            **kwargs: Các tham số bổ sung khác tùy theo nguồn

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        pass

    async def crawl_range(
        self,
        start_page: int = 1,
        end_page: int = 5,
        limit: int = 50,
        callback: Optional[Callable] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Crawl nhiều trang danh sách

        Args:
            start_page: Trang bắt đầu
            end_page: Trang kết thúc
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả
            **kwargs: Các tham số bổ sung khác tùy theo nguồn

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        total_results = {"total": 0, "successful": 0, "failed": 0}
        tasks = []

        # Tạo semaphore để giới hạn số lượng tasks chạy đồng thời
        sem = asyncio.Semaphore(self.max_concurrent)

        async def process_page(page_number):
            async with sem:
                # Thêm độ trễ ngẫu nhiên để tránh bị chặn
                delay = self.get_random_delay(1, 2)
                if page_number > start_page:
                    await asyncio.sleep(delay)

                logger.info(f"Crawling page {page_number}...")
                result = await self.crawl_listings(
                    page=page_number, limit=limit, callback=callback, **kwargs
                )

                return result

        # Tạo task cho mỗi trang
        for page in range(start_page, end_page + 1):
            tasks.append(asyncio.create_task(process_page(page)))

        # Chạy tất cả các tasks và đợi kết quả
        results = await asyncio.gather(*tasks)

        # Tổng hợp kết quả
        for result in results:
            total_results["total"] += result["total"]
            total_results["successful"] += result["successful"]
            total_results["failed"] += result["failed"]

        logger.info(
            f"Range crawl summary: {end_page-start_page+1} pages, {total_results['total']} listings found, "
            f"{total_results['successful']} successful, {total_results['failed']} failed"
        )
        return total_results
