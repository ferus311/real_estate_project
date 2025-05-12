import asyncio
import random
from typing import List, Optional, Callable, Dict, Any
from playwright.async_api import async_playwright

from common.base.base_crawler import BaseCrawler
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from sources.batdongsan.playwright.extractors import extract_list_items


class BatdongsanCrawler(BaseCrawler):
    """
    Crawler danh sách cho Batdongsan sử dụng Playwright
    """

    def __init__(self, max_concurrent: int = 5):
        super().__init__(source="batdongsan", max_concurrent=max_concurrent)
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        ]
        self.max_retries = 3
        self.batch_size = 50

    async def crawl_page(self, page_number: int) -> List[Dict[str, Any]]:
        """
        Crawl một trang danh sách từ Batdongsan

        Args:
            page_number: Số trang cần crawl

        Returns:
            List[Dict]: Danh sách các items đã crawl được
        """
        retries = self.max_retries

        # Kiểm tra checkpoint
        checkpoint = load_checkpoint(self.checkpoint_file)
        if str(page_number) in checkpoint and checkpoint[str(page_number)]:
            print(f"[Batdongsan List] Page {page_number} already crawled")
            return []

        while retries > 0:
            browser = None
            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(headless=True)
                    context = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        viewport={"width": 1280, "height": 720},
                    )

                    # Tối ưu performance bằng cách block các resource không cần thiết
                    await context.route(
                        "**/*",
                        lambda route: (
                            route.abort()
                            if route.request.resource_type
                            in ["image", "stylesheet", "font"]
                            else route.continue_()
                        ),
                    )

                    page = await context.new_page()
                    url = f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}"
                    print(f"[Batdongsan List] Crawling page {page_number}: {url}")

                    await page.goto(url, timeout=60000)
                    html = await page.content()
                    listings = extract_list_items(html)

                    if not listings:
                        print(f"[Batdongsan List] Page {page_number} - No data found")
                        save_checkpoint(
                            self.checkpoint_file, page_number, success=False
                        )
                        return []

                    print(
                        f"[Batdongsan List] Page {page_number} - {len(listings)} listings"
                    )
                    save_checkpoint(self.checkpoint_file, page_number, success=True)

                    # Chuyển đối tượng thành dictionary
                    return [
                        item.__dict__ if hasattr(item, "__dict__") else item
                        for item in listings
                    ]

            except Exception as e:
                print(f"[Batdongsan List] Error on page {page_number}: {e}")
                retries -= 1
                await asyncio.sleep(self.get_random_delay(1, 3))

            finally:
                if browser:
                    try:
                        await browser.close()
                    except Exception as e:
                        print(f"[Batdongsan List] Error closing browser: {e}")

        # Đánh dấu thất bại trong checkpoint
        save_checkpoint(self.checkpoint_file, page_number, success=False)
        return []

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
        if not items:
            return 0

        urls_batch = []
        processed_count = 0

        for item in items:
            if "link" in item and item["link"]:
                urls_batch.append(item["link"])
                processed_count += 1

                # Khi đủ batch size, gửi lên Kafka và xóa batch
                if len(urls_batch) >= self.batch_size and callback:
                    await callback(urls_batch)
                    urls_batch = []

        # Gửi batch cuối cùng nếu còn
        if urls_batch and callback:
            await callback(urls_batch)

        return processed_count
