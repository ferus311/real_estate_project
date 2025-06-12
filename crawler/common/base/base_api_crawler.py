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
        stop_on_empty: bool = True,
        max_empty_pages: int = 2,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Crawl nhiều trang danh sách

        Args:
            start_page: Trang bắt đầu
            end_page: Trang kết thúc
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả
            stop_on_empty: Dừng crawl khi gặp trang không có kết quả (mặc định: True)
            max_empty_pages: Số trang rỗng liên tiếp tối đa trước khi dừng (mặc định: 2)
            **kwargs: Các tham số bổ sung khác tùy theo nguồn

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        total_results = {"total": 0, "successful": 0, "failed": 0, "pages_processed": 0}
        consecutive_empty_pages = 0
        should_stop = asyncio.Event()

        # Tạo semaphore để giới hạn số lượng tasks chạy đồng thời
        sem = asyncio.Semaphore(self.max_concurrent)

        async def process_page(page_number):
            # Kiểm tra nếu đã có tín hiệu dừng
            if should_stop.is_set():
                return {"total": 0, "successful": 0, "failed": 0}

            async with sem:
                # Thêm độ trễ ngẫu nhiên để tránh bị chặn
                delay = self.get_random_delay(1, 2)
                if page_number > start_page:
                    await asyncio.sleep(delay)

                # Nếu đã có tín hiệu dừng sau khi sleep, bỏ qua trang này
                if should_stop.is_set():
                    return {"total": 0, "successful": 0, "failed": 0}

                logger.info(f"Crawling page {page_number}...")
                result = await self.crawl_listings(
                    page=page_number, limit=limit, callback=callback, **kwargs
                )

                # Kiểm tra nếu trang trống (không có kết quả)
                if stop_on_empty and result and result.get("total", 0) == 0:
                    nonlocal consecutive_empty_pages
                    consecutive_empty_pages += 1
                    logger.warning(
                        f"Page {page_number} is empty. Consecutive empty pages: {consecutive_empty_pages}/{max_empty_pages}"
                    )

                    # Nếu đạt đến ngưỡng trang rỗng liên tiếp, đặt tín hiệu dừng
                    if consecutive_empty_pages >= max_empty_pages:
                        logger.warning(
                            f"Reached {max_empty_pages} consecutive empty pages. Stopping crawl process..."
                        )
                        should_stop.set()
                else:
                    # Reset bộ đếm nếu trang không rỗng
                    consecutive_empty_pages = 0

                return result

        # Sử dụng sequential processing để có thể dừng sớm
        results = []
        for page in range(start_page, end_page + 1):
            if should_stop.is_set():
                logger.info(f"Stopping crawl at page {page} due to empty pages")
                break

            result = await process_page(page)
            results.append(result)
            total_results["pages_processed"] += 1

        # Tổng hợp kết quả
        for result in results:
            total_results["total"] += result.get("total", 0)
            total_results["successful"] += result.get("successful", 0)
            total_results["failed"] += result.get("failed", 0)

        logger.info(
            f"Range crawl summary: {total_results['pages_processed']} pages processed out of {end_page-start_page+1}, "
            f"{total_results['total']} listings found, "
            f"{total_results['successful']} successful, {total_results['failed']} failed"
        )

        if should_stop.is_set():
            logger.info("Crawl process was stopped early due to empty pages")

        return total_results
