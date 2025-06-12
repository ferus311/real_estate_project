import asyncio
import random
from typing import List, Optional, Callable, Dict, Any
from playwright.async_api import async_playwright
from playwright._impl._errors import TargetClosedError


from common.base.base_list_crawler import BaseListCrawler
from common.utils.checkpoint import get_checkpoint_manager
from sources.batdongsan.playwright.extractors import extract_list_items


class BatdongsanListCrawler(BaseListCrawler):
    """
    Crawler danh sách cho Batdongsan sử dụng Playwright
    """

    def __init__(self, max_concurrent: int = 5):
        super().__init__(source="batdongsan", max_concurrent=max_concurrent)

        # Khởi tạo checkpoint manager
        self.checkpoint_mgr = get_checkpoint_manager("batdongsan_list")

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

        # Kiểm tra force crawl với checkpoint manager
        if self.force_crawl and hasattr(self, "force_crawl_interval"):
            if not self.checkpoint_mgr.should_force_crawl(
                str(page_number), self.force_crawl_interval
            ):
                print(
                    f"[Batdongsan List] Skipping page {page_number} (already crawled)"
                )
                return []
        elif not self.force_crawl:
            if self.checkpoint_mgr.is_crawled(str(page_number)):
                print(
                    f"[Batdongsan List] Skipping page {page_number} (already crawled)"
                )
                return []

        async def handle_abort(route):
            try:
                await route.abort()
            except TargetClosedError:
                pass  # Page đã đóng, bỏ qua

        async def handle_route(route):
            try:
                if route.request.resource_type in ["image", "stylesheet", "font"]:
                    await route.abort()
                else:
                    await route.continue_()
            except TargetClosedError:
                pass  # Page đã đóng, bỏ qua

        while retries > 0:
            browser = None
            try:
                async with async_playwright() as playwright:
                    # Thêm các tùy chọn để tăng độ ổn định khi chạy trong Docker/Airflow
                    browser = await playwright.chromium.launch(
                        headless=True,
                        args=[
                            "--disable-dev-shm-usage",  # Giải quyết vấn đề bộ nhớ của Docker
                            "--no-sandbox",  # Cần thiết trong một số môi trường container
                            "--disable-setuid-sandbox",
                            "--disable-gpu",
                            "--disable-software-rasterizer",
                        ],
                    )
                    context = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        viewport={"width": 1280, "height": 720},
                        ignore_https_errors=True,  # Bỏ qua lỗi SSL
                    )

                    # Thay vì block tất cả resource, tạo page trước rồi cấu hình trực tiếp trên page
                    # để tránh route handler treo khi đóng context
                    page = await context.new_page()

                    # Đảm bảo kết nối online
                    await page.context.set_offline(False)

                    await page.route(
                        "**/*.{png,jpg,jpeg,webp,svg,gif,css,woff,woff2,ttf,otf}",
                        handle_abort,
                    )
                    await page.route("**/*", handle_route)

                    url = (
                        f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}?cIds=163"
                    )
                    print(f"[Batdongsan List] Crawling page {page_number}: {url}")

                    try:
                        # Tăng timeout và thêm xử lý lỗi khi điều hướng
                        response = await page.goto(
                            url, timeout=90000, wait_until="domcontentloaded"
                        )
                        if response is None or not response.ok:
                            print(
                                f"[Batdongsan List] Failed to load page {page_number}: HTTP status {response.status if response else 'Unknown'}"
                            )
                            # Thử đợi thêm thời gian nếu trang đang tải
                            await page.wait_for_timeout(5000)

                        # Đợi thêm nội dung tải động
                        await page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception as e:
                        print(f"[Batdongsan List] Error during page navigation: {e}")
                        # Vẫn tiếp tục lấy nội dung nếu có

                    # Lấy HTML kể cả khi có lỗi điều hướng
                    html = await page.content()
                    listings = extract_list_items(html)

                    await page.unroute_all(behavior="ignoreErrors")

                    if not listings:
                        print(f"[Batdongsan List] Page {page_number} - No data found")
                        self.checkpoint_mgr.save_checkpoint(
                            str(page_number), success=False
                        )
                        return []

                    print(
                        f"[Batdongsan List] Page {page_number} - {len(listings)} listings"
                    )
                    self.checkpoint_mgr.save_checkpoint(str(page_number), success=True)

                    # Chuyển đối tượng thành dictionary
                    return [
                        item.__dict__ if hasattr(item, "__dict__") else item
                        for item in listings
                    ]

            except Exception as e:
                print(f"[Batdongsan List] Error on page {page_number}: {e}")
                retries -= 1
                # Tăng thời gian chờ với mỗi lần thử lại
                delay = self.get_random_delay(
                    1 + (self.max_retries - retries), 3 + (self.max_retries - retries)
                )
                print(
                    f"[Batdongsan List] Retrying in {delay:.2f} seconds ({retries} attempts left)"
                )
                await asyncio.sleep(delay)

            finally:
                # Đảm bảo browser luôn được đóng để tránh rò rỉ tài nguyên
                if browser:
                    try:
                        # Sử dụng timeout để tránh treo khi đóng browser
                        await asyncio.wait_for(browser.close(), timeout=5.0)
                    except asyncio.TimeoutError:
                        print(f"[Batdongsan List] Timeout when closing browser")
                    except Exception as e:
                        print(f"[Batdongsan List] Error closing browser: {e}")

        # Đánh dấu thất bại trong checkpoint
        self.checkpoint_mgr.save_checkpoint(str(page_number), success=False)
        return []

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
