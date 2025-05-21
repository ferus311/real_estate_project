import asyncio
import random
import json
import logging
from typing import Dict, Any, Optional, List, Callable
from urllib.parse import urlparse
from datetime import datetime
from playwright.async_api import async_playwright

from common.base.base_detail_crawler import BaseDetailCrawler
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from sources.batdongsan.playwright.extractors import extract_detail_info

# Configure logging
logger = logging.getLogger(__name__)


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
                logger.info(f"[Batdongsan Detail] Post {post_id} already crawled")
                return {"skipped": True, "url": url}

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

                    # Chỉ chặn các định dạng tài nguyên không cần thiết để cải thiện hiệu suất
                    await page.route(
                        "**/*.{png,jpg,jpeg,webp,svg,gif,css,woff,woff2,ttf,otf}",
                        lambda route: route.abort(),
                    )

                    # Cho phép tất cả các request khác đi qua mà không cần options
                    await page.route(
                        "**/*",
                        lambda route: (
                            route.continue_()
                            if not route.request.resource_type
                            in ["image", "stylesheet", "font"]
                            else route.abort()
                        ),
                    )
                    logger.info(f"[Batdongsan Detail] Crawling {url}")

                    try:
                        # Tăng timeout và thêm xử lý lỗi khi điều hướng
                        response = await page.goto(
                            url, timeout=60000, wait_until="domcontentloaded"
                        )
                        if response is None or not response.ok:
                            logger.warning(
                                f"[Batdongsan Detail] Failed to load page {url}: HTTP status {response.status if response else 'Unknown'}"
                            )
                            # Thử đợi thêm thời gian nếu trang đang tải
                            await page.wait_for_timeout(5000)

                        # Đợi thêm nội dung tải động với timeout ngắn hơn
                        try:
                            await page.wait_for_load_state("networkidle", timeout=20000)
                        except Exception as e:
                            logger.warning(
                                f"[Batdongsan Detail] Timeout waiting for networkidle: {e}"
                            )
                    except Exception as e:
                        logger.error(
                            f"[Batdongsan Detail] Error during page navigation: {e}"
                        )
                        # Vẫn tiếp tục lấy nội dung nếu có

                    # Lấy HTML content
                    html = await page.content()

                    # Debug check - save HTML to file if it's potentially causing issues
                    if html and len(html) < 1000:  # If HTML is suspiciously short
                        logger.warning(
                            f"[Batdongsan Detail] Suspicious HTML content (length: {len(html)})"
                        )
                        with open(
                            f"/tmp/bds_debug_{post_id or 'unknown'}.html", "w"
                        ) as f:
                            f.write(html)

                    # Extract detail information
                    try:
                        detail_info = extract_detail_info(html)
                    except Exception as extraction_error:
                        logger.error(
                            f"[Batdongsan Detail] Error extracting details: {extraction_error}"
                        )
                        retries -= 1
                        await asyncio.sleep(self.get_random_delay())
                        continue

                    if not detail_info:
                        logger.warning(
                            f"[Batdongsan Detail] No data extracted from {url}"
                        )
                        retries -= 1
                        await asyncio.sleep(self.get_random_delay())
                        continue

                    # Lưu checkpoint nếu có post_id
                    if post_id:
                        save_checkpoint(self.checkpoint_file, post_id, success=True)

                    # Chuyển đối tượng thành dictionary và validate
                    try:
                        if hasattr(detail_info, "__dict__"):
                            detail_dict = detail_info.__dict__
                        else:
                            detail_dict = detail_info

                        # Validate that the dictionary can be JSON serialized
                        json_str = json.dumps(detail_dict)
                        # Test parsing to ensure it's valid JSON
                        json.loads(json_str)

                        # Add the URL to the result
                        detail_dict["url"] = url

                        return detail_dict
                    except (TypeError, json.JSONDecodeError) as json_error:
                        logger.error(
                            f"[Batdongsan Detail] JSON serialization error: {json_error}"
                        )
                        logger.error(
                            f"[Batdongsan Detail] Problem with data: {detail_info}"
                        )
                        retries -= 1
                        await asyncio.sleep(self.get_random_delay())
                        continue

            except Exception as e:
                logger.error(f"[Batdongsan Detail] Error crawling {url}: {e}")
                retries -= 1
                # Tăng thời gian chờ với mỗi lần thử lại
                delay = self.get_random_delay(
                    1 + (self.max_retries - retries), 3 + (self.max_retries - retries)
                )
                logger.info(
                    f"[Batdongsan Detail] Retrying in {delay:.2f} seconds ({retries} attempts left)"
                )
                await asyncio.sleep(delay)

            finally:
                # Đảm bảo browser luôn được đóng để tránh rò rỉ tài nguyên
                if browser:
                    try:
                        # Sử dụng timeout để tránh treo khi đóng browser
                        await asyncio.wait_for(browser.close(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[Batdongsan Detail] Timeout when closing browser"
                        )
                    except Exception as e:
                        logger.error(f"[Batdongsan Detail] Error closing browser: {e}")

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
