import asyncio
import random
from typing import Dict, Any, Optional, List, Callable
from urllib.parse import urlparse
from datetime import datetime
from playwright.async_api import async_playwright

from common.base.base_detail_crawler import BaseDetailCrawler
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from sources.batdongsan.playwright.extractors import extract_detail_info


class BatdongsanDetailCrawler(BaseDetailCrawler):
    """
    Crawler chi tiết cho Batdongsan sử dụng Playwright
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

    def _extract_post_id(self, url: str) -> Optional[str]:
        """Trích xuất post_id từ URL"""
        try:
            path = urlparse(url).path
            post_id = path.split("-")[-1].split("pr")[1] if "pr" in path else None
            return post_id
        except Exception:
            return None

    async def crawl_detail(self, url: str) -> Optional[Dict[str, Any]]:
        """Crawl chi tiết một tin đăng bất động sản từ URL"""
        post_id = self._extract_post_id(url)
        retries = self.max_retries

        # Kiểm tra checkpoint nếu có post_id
        if post_id:
            checkpoint = load_checkpoint(self.checkpoint_file)
            if post_id in checkpoint and checkpoint[post_id]:
                print(f"[Batdongsan Detail] Post {post_id} already crawled")
                return {"skipped": True, "url": url}

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
                    print(f"[Batdongsan Detail] Crawling {url}")

                    await page.goto(url, timeout=60000)
                    await page.wait_for_load_state("networkidle")

                    html = await page.content()
                    detail_info = extract_detail_info(html)

                    if not detail_info:
                        print(f"[Batdongsan Detail] No data extracted from {url}")
                        retries -= 1
                        await asyncio.sleep(self.get_random_delay())
                        continue

                    # Lưu checkpoint nếu có post_id
                    if post_id:
                        save_checkpoint(self.checkpoint_file, post_id, success=True)

                    # Chuyển đối tượng thành dictionary
                    detail_dict = (
                        detail_info.__dict__
                        if hasattr(detail_info, "__dict__")
                        else detail_info
                    )

                    # Thêm metadata
                    detail_dict.update(
                        {
                            "crawl_timestamp": datetime.now().isoformat(),
                            "source": self.source,
                            "url": url,
                        }
                    )

                    return detail_dict

            except Exception as e:
                print(f"[Batdongsan Detail] Error crawling {url}: {e}")
                retries -= 1
                await asyncio.sleep(self.get_random_delay(1, 3))

            finally:
                if browser:
                    try:
                        await browser.close()
                    except Exception as e:
                        print(f"[Batdongsan Detail] Error closing browser: {e}")

        # Đánh dấu thất bại trong checkpoint
        if post_id:
            save_checkpoint(self.checkpoint_file, post_id, success=False)

        return None

    async def process_result(
        self, result: Dict[str, Any], callback: Optional[Callable] = None
    ) -> bool:
        """Xử lý kết quả crawl"""
        if not result:
            return False

        # Nếu đã skip, không cần xử lý
        if result.get("skipped"):
            return True

        # Gọi callback nếu có
        if callback:
            await callback(result)

        return True
