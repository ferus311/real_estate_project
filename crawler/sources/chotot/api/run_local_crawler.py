#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import json
import csv
import argparse
from datetime import datetime
from typing import List, Dict, Any

# Thêm đường dẫn gốc vào sys.path để import được các module
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../.."))
sys.path.append(project_root)



from crawler.sources.chotot.api.detail_crawler_impl import ChototDetailCrawler


async def crawl_listings(
    max_pages: int = 3, limit: int = 50, category: str = "1000"
) -> List[str]:
    """
    Crawl danh sách các tin đăng từ Chợ Tốt

    Args:
        max_pages: Số trang tối đa cần crawl
        limit: Số lượng tin mỗi trang
        category: ID danh mục (mặc định: 1000 - Bất động sản)

    Returns:
        List[str]: Danh sách các URL tin đăng
    """
    print(
        f"Đang crawl danh sách tin từ Chợ Tốt ({max_pages} trang, {limit} tin/trang)..."
    )

    crawler = ChototDetailCrawler()
    all_listings = []

    for page in range(1, max_pages + 1):
        print(f"Crawl trang {page}...")
        try:
            listings = await crawler.crawl_listings(
                page=page, limit=limit, category=category
            )
            if listings:
                print(f"- Tìm thấy {len(listings)} tin")
                all_listings.extend(listings)
            else:
                print("- Không tìm thấy tin nào, dừng crawl")
                break
        except Exception as e:
            print(f"Lỗi khi crawl trang {page}: {e}")
            break

    print(f"Tổng cộng: {len(all_listings)} tin")

    # Chuyển đổi listing IDs thành URLs
    urls = [f"https://nha.chotot.com/{listing_id}" for listing_id in all_listings]
    return urls


async def crawl_details(urls: List[str], max_items: int = 50) -> List[Dict[str, Any]]:
    """
    Crawl chi tiết các tin đăng từ danh sách URLs

    Args:
        urls: Danh sách URLs cần crawl
        max_items: Số lượng tin tối đa cần crawl

    Returns:
        List[Dict[str, Any]]: Danh sách các thông tin chi tiết của tin đăng
    """
    # Giới hạn số lượng URLs
    urls = urls[:max_items]
    print(f"Đang crawl chi tiết {len(urls)} tin đăng từ Chợ Tốt...")

    crawler = ChototDetailCrawler(max_concurrent=5)
    results = []

    # Hàm callback để lưu kết quả
    async def save_result(result):
        if result and not result.get("skipped"):
            results.append(result)
            print(f"Đã crawl: {result.get('title', 'Unknown')}")

    # Crawl batch với callback
    stats = await crawler.crawl_batch(urls, save_result)

    print(f"Kết quả crawl chi tiết:")
    print(f"- Tổng: {stats['total']}")
    print(f"- Thành công: {stats['successful']}")
    print(f"- Thất bại: {stats['failed']}")

    return results


def save_to_csv(data: List[Dict[str, Any]], output_file: str = None) -> str:
    """
    Lưu dữ liệu vào file CSV

    Args:
        data: Dữ liệu cần lưu
        output_file: Tên file output (nếu None sẽ tự tạo)

    Returns:
        str: Đường dẫn đến file CSV đã lưu
    """
    if not data:
        print("Không có dữ liệu để lưu")
        return None

    # Tạo tên file nếu không được cung cấp
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(os.getcwd(), f"chotot_data_{timestamp}.csv")

    # Xác định các trường cần lưu từ dữ liệu
    # Lấy tất cả các keys từ item đầu tiên
    fieldnames = list(data[0].keys())

    # Loại bỏ một số trường không cần thiết hoặc quá dài
    fields_to_exclude = ["description", "images"]
    fieldnames = [f for f in fieldnames if f not in fields_to_exclude]

    # Sắp xếp lại fieldnames để các trường quan trọng ở đầu
    important_fields = [
        "listing_id",
        "title",
        "price",
        "price_string",
        "area",
        "area_string",
        "address",
        "district",
        "region",
        "ward",
        "bedrooms",
        "bathrooms",
        "url",
        "source",
        "post_time",
    ]

    # Giữ lại thứ tự các trường quan trọng và thêm các trường còn lại
    ordered_fields = [f for f in important_fields if f in fieldnames]
    ordered_fields.extend([f for f in fieldnames if f not in important_fields])

    print(f"Đang lưu {len(data)} bản ghi vào {output_file}...")

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
            writer.writeheader()

            for item in data:
                # Lọc bỏ các trường không cần thiết
                filtered_item = {k: item.get(k, "") for k in ordered_fields}
                writer.writerow(filtered_item)

        print(f"Đã lưu dữ liệu thành công vào {output_file}")
        return output_file

    except Exception as e:
        print(f"Lỗi khi lưu file CSV: {e}")
        return None


async def main():
    parser = argparse.ArgumentParser(
        description="Crawl dữ liệu từ Chợ Tốt và lưu vào CSV"
    )
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Số trang tối đa cần crawl"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=10,
        help="Số lượng tin chi tiết tối đa cần crawl",
    )
    parser.add_argument("--output", type=str, help="Đường dẫn file CSV output")
    parser.add_argument(
        "--category",
        type=str,
        default="1000",
        help="ID danh mục (mặc định: 1000 - Bất động sản)",
    )

    args = parser.parse_args()

    # Crawl danh sách tin
    urls = await crawl_listings(max_pages=args.max_pages, category=args.category)

    if not urls:
        print("Không tìm thấy tin nào để crawl chi tiết")
        return

    # Crawl chi tiết
    results = await crawl_details(urls, max_items=args.max_items)

    if not results:
        print("Không có kết quả chi tiết để lưu")
        return

    # Lưu kết quả vào CSV
    output_file = save_to_csv(results, args.output)

    if output_file:
        print(f"Quá trình crawl hoàn tất. Dữ liệu đã được lưu vào: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
