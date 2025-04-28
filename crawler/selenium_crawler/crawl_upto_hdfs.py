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

from concurrent.futures import ThreadPoolExecutor, as_completed

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
HDFS_DIR = "/data/staging/"
hdfs_client = InsecureClient(HDFS_URL, user="airflow")

CHECKPOINT_FILE = "/tmp/checkpoint.json"
BATCH_SIZE = 50
buffer = []


# Initialize Chrome WebDriver
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        f"--user-data-dir=/tmp/chrome_user_data_{random.randint(1000, 9999)}"
    )  # Thư mục duy nhất
    service = Service("/usr/local/bin/chromedriver")
    return webdriver.Chrome(service=service, options=options)


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
        # Đảm bảo thư mục tồn tại trước khi ghi log
        if not hdfs_client.status(log_dir, strict=False):
            hdfs_client.makedirs(log_dir)

        # Ghi log
        with hdfs_client.write(log_file, append=True, encoding="utf-8") as writer:
            writer.write(log_message)

        print(f"Logged error to {log_file}")

    except Exception as e:
        print(f"Failed to log error to HDFS: {e}")


# Save data to HDFS in Parquet format (Write to new file each batch)
def write_to_parquet(houses_json):
    df = pd.DataFrame(houses_json)
    table = pa.Table.from_pandas(df)

    # Tạo file tạm trên máy cục bộ
    local_temp_file = f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    pq.write_table(table, local_temp_file)

    # Đường dẫn trên HDFS
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


def get_unique_batch_name():
    """Tạo tên batch duy nhất theo ngày tháng và thời gian"""
    date_str = datetime.now().strftime("%Y/%m/%d")  # Tạo thư mục theo ngày
    directory = f"/data/crawl_batch/{date_str}"

    # Tạo thư mục nếu chưa có
    if not hdfs_client.status(directory, strict=False):
        hdfs_client.makedirs(directory)

    # Tạo tên batch với timestamp để đảm bảo tính duy nhất
    batch_name = f"{directory}/batch_{int(time.time())}.parquet"
    return batch_name


# Batch upload to HDFS (Now creates a new file for each batch)
def batch_upload_to_hdfs():
    global buffer
    if buffer:
        combined_data = [house for page_data in buffer for house in page_data]
        try:
            write_to_parquet(combined_data)  # Gọi hàm đúng
            buffer = []  # Reset buffer sau khi ghi thành công
        except Exception as e:
            log_error_to_hdfs(f"Failed to upload batch to HDFS: {e}")
            print(f"Failed to upload batch to HDFS: {e}")


# Crawl a single page
def crawl_page(page_number):
    retries = 3
    while retries > 0:
        try:
            driver = init_driver()
            url = f"https://batdongsan.com.vn/nha-dat-ban/p{page_number}"
            print(f"Crawling page: {page_number}")
            navigateToWeb(url, driver)
            houses_json = getJsonHouses(driver)
            print(f"Found {len(houses_json)} houses on page {page_number}")

            if not houses_json:
                print(f"No data found for page {page_number}. Marking as failed.")
                save_checkpoint(page_number, success=False)
                return f"Page {page_number} failed due to no data."

            buffer.append(houses_json)
            # print(f"Buffer size: {len(buffer)}")
            if len(buffer) >= BATCH_SIZE:
                batch_upload_to_hdfs()

            save_checkpoint(page_number, success=True)
            return f"Page {page_number} completed successfully."

        except Exception as e:
            log_error_to_hdfs(f"Error on page {page_number}: {e}")
            print(f"Error on page {page_number}: {e}")
            retries -= 1
            time.sleep(5)
        finally:
            driver.quit()

    save_checkpoint(page_number, success=False)
    return f"Page {page_number} failed after multiple retries."


def crawl_data_multithreaded(start_page, end_page, max_threads=5, force_crawl=False):
    last_checkpoint = load_checkpoint()
    if force_crawl:
        # Bỏ qua checkpoint, crawl lại tất cả các trang
        pages_to_crawl = list(range(start_page, end_page + 1))
    else:
        # Chỉ crawl các trang chưa thành công trong checkpoint
        pages_to_crawl = [
            p
            for p in range(start_page, end_page + 1)
            if str(p) not in last_checkpoint or not last_checkpoint[str(p)]
        ]

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_page = {
            executor.submit(crawl_page, page): page for page in pages_to_crawl
        }
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                print(future.result())
            except Exception as e:
                print(f"Error occurred while crawling page {page}: {e}")


if __name__ == "__main__":
    MAX_THREADS = 7
    # Sử dụng argparse để xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description="Crawl real estate data.")
    parser.add_argument(
        "--start_page",
        type=int,
        default=0,
        help="Trang bắt đầu crawl (mặc định là 0).",
    )
    parser.add_argument(
        "--end_page",
        type=int,
        default=500,
        help="Trang kết thúc crawl (mặc định là 500).",
    )
    parser.add_argument(
        "--retry_failed",
        action="store_true",
        help="Chỉ crawl lại các trang bị lỗi từ checkpoint.",
    )

    args = parser.parse_args()

    START_PAGE = args.start_page
    END_PAGE = args.end_page
    RETRY_FAILED = args.retry_failed

    if RETRY_FAILED:
        # Crawl lại các trang bị lỗi từ checkpoint
        print("Crawl lại các trang bị lỗi...")
        last_checkpoint = load_checkpoint()
        failed_pages = [
            int(page) for page, success in last_checkpoint.items() if not success
        ]
        if failed_pages:
            print(f"Các trang bị lỗi: {failed_pages}")
            crawl_data_multithreaded(min(failed_pages), max(failed_pages), MAX_THREADS)
        else:
            print("Không có trang nào bị lỗi để crawl lại.")
    else:
        # Crawl từ start_page đến end_page, bỏ qua checkpoint
        print(f"Crawl từ trang {START_PAGE} đến trang {END_PAGE}...")
        crawl_data_multithreaded(START_PAGE, END_PAGE, MAX_THREADS, force_crawl=True)

    # Đảm bảo upload dữ liệu còn lại
    batch_upload_to_hdfs()
    print("Crawling completed.")
