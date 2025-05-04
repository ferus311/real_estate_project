import asyncio
from playwright.async_api import async_playwright
from batdongsan_playwright import *  # Adjusted import path to relative location
# from hdfs import InsecureClient
import json
import random
import os
import time
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import argparse

# HDFS config
# HDFS_URL = "http://namenode:9870"
# HDFS_DIR = "/data/test/raw_data"
# hdfs_client = InsecureClient(HDFS_URL, user="airflow")

CHECKPOINT_FILE = "./tmp/checkpoint.json"
BATCH_SIZE = 10
buffer = []

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

# --- Checkpoint functions ---

def save_checkpoint(page_number, success=True):
    try:
        checkpoint_data = load_checkpoint()
        checkpoint_data[str(page_number)] = success
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
    except Exception as e:
        print(f"Failed to save checkpoint for page {page_number}: {e}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}

# --- Logging ---

# def log_error_to_hdfs(message):
#     date_str = datetime.now().strftime("%Y-%m-%d")
#     log_dir = f"/data/test/logs/{date_str}"
#     log_file = f"{log_dir}/crawler_logs_{datetime.now().strftime('%H_%M')}.log"
#     log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"

#     try:
#         # Đảm bảo thư mục tồn tại
#         if not hdfs_client.status(log_dir, strict=False):
#             hdfs_client.makedirs(log_dir)

#         # Đảm bảo file tồn tại trước khi append
#         if not hdfs_client.status(log_file, strict=False):
#             with hdfs_client.write(log_file, overwrite=True, encoding="utf-8") as writer:
#                 writer.write("")  # Tạo file rỗng

#         # Ghi log
#         with hdfs_client.write(log_file, append=True, encoding="utf-8") as writer:
#             writer.write(log_message)

#         print(f"Logged error to {log_file}")

#     except Exception as e:
#         print(f"Failed to log error to HDFS: {e}")


# --- Save to HDFS ---

def write_to_parquet(houses_json):
    df = pd.DataFrame(houses_json)
    local_folder = "data/"
    os.makedirs(local_folder, exist_ok=True)
    local_file = os.path.join(local_folder, f"batdongsan_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    try:
        df.to_csv(local_file, index=False)
        print(f"✅ Saved to local folder: {local_file}")
    except Exception as e:
        print(f"❌ Save failed: {e}")

async def crawl_page(playwright, page_number):
    retries = 3
    while retries > 0:
        try:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=random.choice(user_agents),
                viewport={"width": 1280, "height": 720},
            )
            page = await context.new_page()

            url = f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}"
            print(f"Crawling page: {page_number}")
            await page.goto(url, timeout=60000)

            houses_json = await getJsonHouses(page)  # getJsonHouses phải async
            print(f"Found {len(houses_json)} houses on page {page_number}")
            logging.debug(f"Found {len(houses_json)} houses on page {page_number}")

            if not houses_json:
                save_checkpoint(page_number, success=False)
                return f"Page {page_number} failed (no data)"

            buffer.append(houses_json)
            if len(buffer) >= BATCH_SIZE:
                write_to_parquet([house for batch in buffer for house in batch])
                print(f"Writing {len(buffer)} houses to HDFS")
                buffer.clear()

            save_checkpoint(page_number, success=True)
            await browser.close()
            return f"Page {page_number} done"
        except Exception as e:
            # log_error_to_hdfs(f"Error on page {page_number}: {e}")
            print(f"Error on page {page_number}: {e}")
            retries -= 1
            await asyncio.sleep(5)
        finally:
            try:
                await browser.close()
            except:
                pass
    save_checkpoint(page_number, success=False)
    return f"Page {page_number} failed after retries"

async def crawl_data(start_page, end_page, max_tasks=5, force_crawl=False):
    last_checkpoint = load_checkpoint()
    if force_crawl:
        pages = list(range(start_page, end_page + 1))
    else:
        pages = [p for p in range(start_page, end_page + 1) if str(p) not in last_checkpoint or not last_checkpoint[str(p)]]

    async with async_playwright() as playwright:
        tasks = []
        sem = asyncio.Semaphore(max_tasks)

        async def sem_task(page_number):
            async with sem:
                return await crawl_page(playwright, page_number)

        for page_number in pages:
            tasks.append(asyncio.create_task(sem_task(page_number)))

        for task in asyncio.as_completed(tasks):
            result = await task
            print(result)

    if buffer:
        write_to_parquet([house for batch in buffer for house in batch])
        buffer.clear()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_page", type=int, default=1)
    parser.add_argument("--end_page", type=int, default=20)
    parser.add_argument("--retry_failed", action="store_true")
    args = parser.parse_args()

    if args.retry_failed:
        last_checkpoint = load_checkpoint()
        failed_pages = [int(p) for p, success in last_checkpoint.items() if not success]
        if failed_pages:
            asyncio.run(crawl_data(min(failed_pages), max(failed_pages), max_tasks=7, force_crawl=False))
        else:
            print("No failed pages to retry.")
    else:
        asyncio.run(crawl_data(args.start_page, args.end_page, max_tasks=7, force_crawl=True))

    print("Crawling completed.")
