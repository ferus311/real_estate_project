import asyncio
import random
from datetime import datetime
from urllib.parse import urlparse
from playwright.async_api import async_playwright

from common.models.house_detail import HouseDetailItem
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from sources.batdongsan.playwright.extractors import extract_detail_info

# Cấu hình
CHECKPOINT_FILE = "./checkpoint/batdongsan_detail_checkpoint.json"
MAX_RETRIES = 3

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

async def crawl_detail(url):
    """
    Crawl chi tiết một tin đăng bất động sản từ URL

    Args:
        url (str): URL của trang chi tiết cần crawl

    Returns:
        dict: Dữ liệu chi tiết tin đăng, hoặc None nếu có lỗi
    """
    retries = MAX_RETRIES
    while retries > 0:
        browser = None
        try:
            # Trích xuất post_id từ URL để sử dụng cho checkpoint
            path = urlparse(url).path
            post_id = path.split('-')[-1].split('pr')[1] if 'pr' in path else None

            if post_id:
                # Kiểm tra xem đã crawl chưa
                checkpoint = load_checkpoint(CHECKPOINT_FILE)
                if post_id in checkpoint and checkpoint[post_id]:
                    print(f"[Detail Crawler] Post {post_id} already crawled")
                    return None

            # Khởi tạo browser và context
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=random.choice(user_agents),
                    viewport={"width": 1280, "height": 720},
                )

                # Chặn tải ảnh, CSS, font để tăng tốc
                await context.route('**/*', lambda route:
                    route.abort() if route.request.resource_type in ['image', 'stylesheet', 'font']
                    else route.continue_()
                )

                # Truy cập trang chi tiết
                page = await context.new_page()
                print(f"[Detail Crawler] Crawling {url}")
                await page.goto(url, timeout=60000)
                await page.wait_for_load_state("networkidle")

                # Lấy HTML và trích xuất thông tin
                html = await page.content()
                detail_info = extract_detail_info(html)

                if not detail_info:
                    print(f"[Detail Crawler] No data extracted from {url}")
                    retries -= 1
                    await asyncio.sleep(2)
                    continue

                # Lưu checkpoint nếu có post_id
                if post_id:
                    save_checkpoint(CHECKPOINT_FILE, post_id, success=True)

                # Chuyển đối tượng thành dictionary
                detail_dict = detail_info.__dict__ if hasattr(detail_info, '__dict__') else detail_info

                # Thêm metadata
                detail_dict.update({
                    "crawl_timestamp": datetime.now().isoformat(),
                    "source": "batdongsan",
                    "url": url
                })

                await browser.close()
                return detail_dict

        except Exception as e:
            print(f"[Detail Crawler] Error crawling {url}: {e}")
            retries -= 1
            await asyncio.sleep(random.uniform(2, 5))  # Random backoff
        finally:
            if browser:
                try:
                    await browser.close()
                except:
                    pass

    # Lưu checkpoint thất bại nếu có post_id
    if 'post_id' in locals() and post_id:
        save_checkpoint(CHECKPOINT_FILE, post_id, success=False)

    return None


# CLI để test
if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description='Crawl chi tiết từ URL')
    parser.add_argument('--url', type=str, required=True, help='URL to crawl')

    args = parser.parse_args()

    result = asyncio.run(crawl_detail(args.url))
    if result:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("Failed to crawl detail")
