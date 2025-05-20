from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Callable
import asyncio
import random
from datetime import datetime


class BaseDetailCrawler(ABC):
    """
    Lớp cơ sở cho tất cả các detail crawler
    """

    def __init__(self, source: str, max_concurrent: int = 5):
        import os

        self.source = source
        self.max_concurrent = max_concurrent
        self.running = True

        # Đảm bảo thư mục checkpoint tồn tại
        checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "./checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)
        self.checkpoint_file = os.path.join(
            checkpoint_dir, f"{source}_detail_checkpoint.json"
        )

    @abstractmethod
    async def crawl_detail(self, url: str) -> Dict[str, Any]:
        """
        Crawl chi tiết từ một URL

        Args:
            url: URL cần crawl

        Returns:
            Dict: Dữ liệu chi tiết, hoặc None nếu có lỗi
        """
        pass

    @abstractmethod
    async def process_result(
        self, result: Dict[str, Any], callback: Optional[Callable] = None
    ) -> bool:
        """
        Xử lý kết quả crawl

        Args:
            result: Kết quả crawl
            callback: Hàm callback để xử lý kết quả

        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        pass

    async def crawl_batch(
        self, urls: List[str], callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Crawl một batch các URLs

        Args:
            urls: Danh sách URLs cần crawl
            callback: Hàm callback để xử lý kết quả

        Returns:
            Dict: Thống kê kết quả crawl
        """
        sem = asyncio.Semaphore(self.max_concurrent)
        results = {"total": len(urls), "successful": 0, "failed": 0}

        async def process_url(url: str) -> bool:
            async with sem:
                try:
                    result = await self.crawl_detail(url)
                    if result:
                        success = await self.process_result(result, callback)
                        return success
                    return False
                except Exception as e:
                    print(f"Error crawling {url}: {e}")
                    return False

        tasks = [process_url(url) for url in urls]
        task_results = await asyncio.gather(*tasks)

        results["successful"] = sum(1 for r in task_results if r)
        results["failed"] = results["total"] - results["successful"]

        return results

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
