import asyncio
import random
from datetime import datetime
from playwright.async_api import async_playwright

from crawler.common.utils.checkpoint import load_checkpoint, save_checkpoint
from crawler.common.storage.local_storage import write_to_local
from crawler.sources.batdongsan.playwright.extractors import extract_list_items

# Cấu hình
CHECKPOINT_FILE = "./tmp/batdongsan_list_checkpoint.json"
BATCH_SIZE = 1000
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

buffer = []


# Crawl 1 trang cụ thể
async def crawl_list_page(playwright, page_number: int):
    retries = MAX_RETRIES
    while retries > 0:
        try:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                viewport={"width": 1280, "height": 720},
            )
            page = await context.new_page()
            url = f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}"
            print(f"[List Crawler] Crawling page {page_number}: {url}")
            await page.goto(url, timeout=60000)

            listings = await extract_list_items(page)

            if not listings:
                print(f"[List Crawler] Page {page_number} - No data found")
                save_checkpoint(CHECKPOINT_FILE, page_number, success=False)
                return

            for item in listings:
                buffer.append(item.__dict__)

            print(f"[List Crawler] Page {page_number} - {len(listings)} listings")

            if len(buffer) >= BATCH_SIZE:
                write_to_local(buffer, prefix="house_list", file_format="csv")
                buffer.clear()

            save_checkpoint(CHECKPOINT_FILE, page_number, success=True)
            await browser.close()
            return
        except Exception as e:
            # log_error_to_hdfs(f"List crawl error on page {page_number}: {e}")
            print(f"❌ Error on page {page_number}: {e}")
            retries -= 1
            await asyncio.sleep(3)
        finally:
            try:
                await browser.close()
            except:
                pass

    save_checkpoint(CHECKPOINT_FILE, page_number, success=False)


# Crawl nhiều trang
async def crawl_list_pages(start_page=1, end_page=100, max_tasks=5, force=False):
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    pages = (
        range(start_page, end_page + 1)
        if force
        else [
            p
            for p in range(start_page, end_page + 1)
            if str(p) not in checkpoint or not checkpoint[str(p)]
        ]
    )

    async with async_playwright() as playwright:
        sem = asyncio.Semaphore(max_tasks)
        tasks = []

        async def sem_task(pn):
            async with sem:
                await crawl_list_page(playwright, pn)

        for page_number in pages:
            tasks.append(asyncio.create_task(sem_task(page_number)))

        await asyncio.gather(*tasks)

        if buffer:
            write_to_local(buffer, prefix="house_list", file_format="csv")
            buffer.clear()


# CLI entry
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    asyncio.run(crawl_list_pages(args.start, args.end, force=args.force))
