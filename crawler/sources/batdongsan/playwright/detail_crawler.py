import asyncio
import random
import pandas as pd
import os
from playwright.async_api import async_playwright

from crawler.common.models.house_detail import HouseDetailItem
from crawler.common.storage.local_storage import (
    write_to_local,
    load_latest_from_local,
    list_all_house_list_files,
    load_links_from_file,
)
from crawler.common.utils.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    load_file_checkpoint,
    mark_file_done,
    is_file_done,
)
from crawler.sources.batdongsan.playwright.extractors import extract_detail_info

# Cấu hình
CHECKPOINT_FILE = "./tmp/batdongsan_detail_checkpoint.json"
BATCH_SIZE = 1000
MAX_RETRIES = 2
MAX_TASKS = 5
FLUSH_INTERVAL = 30  # giây

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]


# ================================
# Crawl 1 link chi tiết
# ================================
async def crawl_detail_link(
    playwright, link: str, sem: asyncio.Semaphore, queue: asyncio.Queue
):
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
                html = await page.content()
                detail = extract_detail_info(html)

                await page.close()
                await context.close()
                await browser.close()

                if detail:
                    await queue.put(detail.__dict__)
                    save_checkpoint(CHECKPOINT_FILE, link, success=True)
                else:
                    save_checkpoint(CHECKPOINT_FILE, link, success=False)
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


# ================================
# Ghi dữ liệu nền
# ================================
async def writer_worker(queue: asyncio.Queue, stop_event: asyncio.Event):
    buffer = []
    while not stop_event.is_set() or not queue.empty():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=FLUSH_INTERVAL)
            buffer.append(item)
        except asyncio.TimeoutError:
            pass  # không có dữ liệu mới, vẫn ghi buffer nếu có

        if len(buffer) >= BATCH_SIZE or (stop_event.is_set() and buffer):
            print(f"💾 Flushing {len(buffer)} records to CSV...")
            write_to_local(buffer, prefix="house_detail_test", file_format="csv")
            buffer.clear()


# ================================
# Crawl nhiều link chi tiết
# ================================
async def crawl_detail_links(links, max_tasks=MAX_TASKS):
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    links = [link for link in links if link not in checkpoint or not checkpoint[link]]
    print(f"🔗 Total uncrawled links: {len(links)}")

    queue = asyncio.Queue()
    stop_event = asyncio.Event()

    async with async_playwright() as playwright:
        sem = asyncio.Semaphore(max_tasks)

        # Launch writer task
        writer_task = asyncio.create_task(writer_worker(queue, stop_event))

        # Launch crawl tasks
        tasks = [
            asyncio.create_task(crawl_detail_link(playwright, link, sem, queue))
            for link in links
        ]
        await asyncio.gather(*tasks)

        # Signal writer to flush and exit
        stop_event.set()
        await writer_task


# ==========================
# Main logic
# ==========================
async def process_file(path, checkpoint):
    filename = os.path.basename(path)
    if is_file_done(filename, checkpoint):
        print(f"✅ Skipping {filename} (already processed)")
        return

    print(f"\n🚀 Crawling from file: {filename}")
    links = load_links_from_file(path)
    await crawl_detail_links(links)
    mark_file_done(filename, CHECKPOINT_FILE)


async def crawl_all_detail_from_all_files():
    files = list_all_house_list_files(prefix="house_list", file_format="csv")
    if not files:
        print("❌ No files found to process.")
        return
    checkpoint = load_file_checkpoint(CHECKPOINT_FILE)

    for file_path in files:
        await process_file(file_path, checkpoint)


# ================================
# CLI entry
# ================================
if __name__ == "__main__":
    asyncio.run(crawl_all_detail_from_all_files())
