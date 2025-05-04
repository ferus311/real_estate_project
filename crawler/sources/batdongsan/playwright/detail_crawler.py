import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright

from crawler.common.models.house_detail import HouseDetailItem
from crawler.common.storage.local_storage import write_to_local, load_latest_from_local
from crawler.common.utils.checkpoint import load_checkpoint, save_checkpoint
from crawler.sources.batdongsan.playwright.extractors import extract_detail_info

# Cấu hình
CHECKPOINT_FILE = "./tmp/batdongsan_detail_checkpoint.json"
BATCH_SIZE = 20
MAX_RETRIES = 2
MAX_TASKS = 3

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


# Crawl 1 link chi tiết
async def crawl_detail_link(playwright, link: str, sem: asyncio.Semaphore):
    async with sem:
        retries = MAX_RETRIES
        checkpoint = load_checkpoint(CHECKPOINT_FILE)
        if link in checkpoint and checkpoint[link]:
            return

        while retries > 0:
            browser = None
            try:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=random.choice(user_agents)
                )
                page = await context.new_page()
                print(f"🔎 Crawling: {link}")
                await page.goto(link, timeout=60000)

                detail = await extract_detail_info(page)
                await page.close()
                await context.close()
                await browser.close()

                if detail:
                    buffer.append(detail.__dict__)
                    save_checkpoint(CHECKPOINT_FILE, link, success=True)
                else:
                    save_checkpoint(CHECKPOINT_FILE, link, success=False)

                if len(buffer) >= BATCH_SIZE:
                    write_to_local(
                        buffer, prefix="house_detail_test", file_format="csv"
                    )
                    buffer.clear()
                return
            except Exception as e:
                print(f"❌ Error on link {link}: {e}")
                retries -= 1
                await asyncio.sleep(2)
            finally:
                try:
                    if browser:
                        await browser.close()
                except:
                    pass

        save_checkpoint(CHECKPOINT_FILE, link, success=False)


# Crawl nhiều link chi tiết
async def crawl_detail_links(links, max_tasks=MAX_TASKS):
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    links = [link for link in links if link not in checkpoint or not checkpoint[link]]

    print(f"🔗 Total uncrawled links: {len(links)}")

    async with async_playwright() as playwright:
        sem = asyncio.Semaphore(max_tasks)
        tasks = [
            asyncio.create_task(crawl_detail_link(playwright, link, sem))
            for link in links
        ]
        await asyncio.gather(*tasks)

    if buffer:
        write_to_local(buffer, prefix="house_detail_test", file_format="csv")
        buffer.clear()


# CLI entry
if __name__ == "__main__":
    df = load_latest_from_local("house_list", "csv")
    links = df["link"].dropna().unique().tolist()[:100]  # test 20 link đầu tiên
    print(f"🚀 Crawling {len(links)} detail links...")
    asyncio.run(crawl_detail_links(links))
