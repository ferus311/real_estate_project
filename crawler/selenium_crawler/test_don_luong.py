from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from batdongsan import *
from hdfs import InsecureClient
import json
import random
import os
import time
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import argparse

# docker build -t realestate-crawler .
# User agent list
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

# HDFS Configuration
HDFS_URL = "http://namenode:9870"
HDFS_DIR = "/raw/crawl_batch"
hdfs_client = InsecureClient(HDFS_URL, user="airflow")

CHECKPOINT_FILE = "/tmp/checkpoint.json"
BATCH_SIZE = 3
buffer = []

# Initialize Chrome WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    # Tạo thư mục profile riêng trong vùng có quyền ghi
    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Save checkpoint
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

# Log errors to HDFS
def log_error_to_hdfs(message):
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_dir = f"/data/crawl_batch/logs/{date_str}"
    log_file = f"{log_dir}/crawler_logs_{datetime.now().strftime('%H_%M')}.log"

    log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"

    try:
        if not hdfs_client.status(log_dir, strict=False):
            hdfs_client.makedirs(log_dir)
        with hdfs_client.write(log_file, append=True, encoding="utf-8") as writer:
            writer.write(log_message)
        print(f"Logged error to {log_file}")
    except Exception as e:
        print(f"Failed to log error to HDFS: {e}")

# Save data to HDFS in Parquet format
def write_to_parquet(houses_json):
    df = pd.DataFrame(houses_json)
    table = pa.Table.from_pandas(df)
    local_temp_file = f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    pq.write_table(table, local_temp_file)

    batch_name = (
        f"{HDFS_DIR}/batdongsan_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    try:
        hdfs_client.upload(batch_name, local_temp_file, overwrite=True)
        print(f"✅ Uploaded to HDFS: {batch_name}")
    except Exception as e:
        log_error_to_hdfs(f"Upload failed: {e}")
        print(f"❌ Upload failed: {e}")
    finally:
        os.remove(local_temp_file)

# Batch upload
def batch_upload_to_hdfs():
    global buffer
    if buffer:
        combined_data = [house for page_data in buffer for house in page_data]
        try:
            write_to_parquet(combined_data)
            buffer = []
        except Exception as e:
            log_error_to_hdfs(f"Failed to upload batch to HDFS: {e}")
            print(f"Failed to upload batch to HDFS: {e}")

# Crawl một trang đơn
def crawl_page(page_number):
    retries = 3
    while retries > 0:
        driver = None
        try:
            driver = init_driver()
            url = f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}"
            print(f"Crawling page: {page_number}")
            navigateToWeb(url, driver)
            houses_json = getJsonHouses(driver)
            print(f"Found {len(houses_json)} houses on page {page_number}")

            if not houses_json:
                print(f"No data found for page {page_number}. Marking as failed.")
                return

            buffer.append(houses_json)
            if len(buffer) >= BATCH_SIZE:
                batch_upload_to_hdfs()

            return

        except Exception as e:
            print(f"Error on page {page_number}: {e}")
            retries -= 1
            time.sleep(3)
        finally:
            if driver:
                driver.quit()

# Chạy tuần tự
def crawl_data_single(start_page, end_page):
    for page in range(start_page, end_page + 1):
        crawl_page(page)
    batch_upload_to_hdfs()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_page", type=int, default=1)
    parser.add_argument("--end_page", type=int, default=5)
    args = parser.parse_args()

    crawl_data_single(args.start_page, args.end_page)
    print("✅ Crawling completed.")
