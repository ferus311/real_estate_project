from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Callable
import asyncio
import aiohttp
import random
from datetime import datetime


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

    @abstractmethod
    async def get_api_url(self, identifier: str) -> str:
        """
        Tạo URL API dựa trên identifier

        Args:
            identifier: Định danh của item cần crawl (ID, URL, etc.)

        Returns:
            str: URL API
        """
        pass

    @abstractmethod
    async def parse_response(
        self, response_data: Dict[str, Any], identifier: str
    ) -> Dict[str, Any]:
        """
        Phân tích dữ liệu API response

        Args:
            response_data: Dữ liệu từ API
            identifier: Định danh của item

        Returns:
            Dict: Dữ liệu đã được phân tích
        """
        pass

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
        retries = 3

        while retries > 0:
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = random.uniform(2, 5)
                        print(f"Rate limited. Waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        retries -= 1
                    else:
                        print(f"API error: {response.status} for {url}")
                        retries -= 1
                        await asyncio.sleep(1)
            except Exception as e:
                print(f"Request error: {e} for {url}")
                retries -= 1
                await asyncio.sleep(1)

        return None

    async def crawl_item(self, identifier: str) -> Optional[Dict[str, Any]]:
        """
        Crawl một item qua API

        Args:
            identifier: Định danh của item

        Returns:
            Dict: Dữ liệu item, hoặc None nếu có lỗi
        """
        try:
            api_url = await self.get_api_url(identifier)

            async with aiohttp.ClientSession() as session:
                response_data = await self.fetch_api(session, api_url)

                if response_data:
                    result = await self.parse_response(response_data, identifier)
                    result.update(
                        {
                            "source": self.source,
                            "crawl_timestamp": datetime.now().isoformat(),
                            "identifier": identifier,
                        }
                    )
                    return result

            return None
        except Exception as e:
            print(f"Error crawling {identifier}: {e}")
            return None

    async def crawl_batch(
        self, identifiers: List[str], callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Crawl một batch các identifiers

        Args:
            identifiers: Danh sách identifiers cần crawl
            callback: Hàm callback để xử lý kết quả

        Returns:
            Dict: Thống kê kết quả crawl
        """
        sem = asyncio.Semaphore(self.max_concurrent)
        results = {"total": len(identifiers), "successful": 0, "failed": 0}

        async def process_identifier(identifier: str) -> bool:
            async with sem:
                try:
                    result = await self.crawl_item(identifier)
                    if result and callback:
                        await callback(result)
                        results["successful"] += 1
                        return True
                    results["failed"] += 1
                    return False
                except Exception as e:
                    print(f"Error processing {identifier}: {e}")
                    results["failed"] += 1
                    return False

        tasks = [process_identifier(id) for id in identifiers]
        await asyncio.gather(*tasks)

        return results
