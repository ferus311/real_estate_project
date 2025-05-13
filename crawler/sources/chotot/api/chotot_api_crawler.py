import asyncio
import json
import re
import aiohttp
import os
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import logging
import random
import html

from common.base.base_api_crawler import BaseApiCrawler
from common.models.house_detail import HouseDetailItem
from common.utils.checkpoint import load_checkpoint, save_checkpoint

logger = logging.getLogger(__name__)


class ChototApiCrawler(BaseApiCrawler):
    """
    Crawler API cho Chotot sử dụng API để lấy dữ liệu bất động sản
    """

    def __init__(self, max_concurrent: int = 10):
        super().__init__(source="chotot", max_concurrent=max_concurrent)
        self.api_base_url = "https://gateway.chotot.com/v1/public/ad-listing"
        self.max_retries = 3

        # Cập nhật headers đặc thù cho Chotot
        self.headers = {
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://nha.chotot.com",
            "Referer": "https://nha.chotot.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _clean_html(self, text: str) -> str:
        """
        Loại bỏ tất cả các thẻ HTML và giải mã các ký tự đặc biệt từ văn bản.

        Args:
            text: Văn bản có thể chứa mã HTML

        Returns:
            str: Văn bản đã được làm sạch
        """
        if not text:
            return ""

        # Loại bỏ tất cả các thẻ HTML
        clean_text = re.sub(r"<[^>]+>", "", text)

        # Giải mã các thực thể HTML như &nbsp;, &amp;, v.v.
        clean_text = html.unescape(clean_text)

        # Thay thế nhiều khoảng trắng liên tiếp bằng một khoảng trắng
        clean_text = re.sub(r"\s+", " ", clean_text)

        return clean_text.strip()

    def get_random_delay(self, min_delay: int = 1, max_delay: int = 3) -> float:
        """
        Tạo độ trễ ngẫu nhiên giữa các lần gọi API

        Args:
            min_delay: Thời gian tối thiểu (giây)
            max_delay: Thời gian tối đa (giây)

        Returns:
            float: Độ trễ ngẫu nhiên
        """
        return random.uniform(min_delay, max_delay)

    async def get_api_url(self, identifier: str) -> str:
        """
        Tạo URL API dựa trên identifier

        Args:
            identifier: URL hoặc ID của listing

        Returns:
            str: URL API
        """
        listing_id = self._extract_listing_id(identifier)
        # Sử dụng kết cấu API v1 với params có ID
        return f"{self.api_base_url}?list_id={listing_id}"

    async def fetch_api(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[Dict[str, Any]]:
        """
        Gọi API và lấy dữ liệu

        Args:
            session: aiohttp session
            url: URL API

        Returns:
            Dict: Dữ liệu API, hoặc None nếu có lỗi
        """
        retries = self.max_retries

        while retries > 0:
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = self.get_random_delay(2, 5)
                        logger.warning(f"Rate limited. Waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        retries -= 1
                    else:
                        logger.error(f"API error: {response.status} for {url}")
                        retries -= 1
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Request error: {e} for {url}")
                retries -= 1
                await asyncio.sleep(1)

        return None

    def _extract_location_from_api(self, ad_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Trích xuất thông tin địa điểm từ dữ liệu API

        Args:
            ad_data: Dữ liệu API của một listing

        Returns:
            Dict[str, str]: Thông tin địa điểm
        """
        # Trả về location dạng string, cách nhau bằng dấu phẩy
        parts = []
        if "ward_name" in ad_data and ad_data["ward_name"]:
            parts.append(ad_data["ward_name"])
        if "area_name" in ad_data and ad_data["area_name"]:
            parts.append(ad_data["area_name"])
        if "region_name" in ad_data and ad_data["region_name"]:
            parts.append(ad_data["region_name"])
        if "street_name" in ad_data and ad_data["street_name"]:
            parts.insert(0, ad_data["street_name"])  # street đứng đầu nếu có

        location = ", ".join(parts)
        return location

    async def crawl_listings(
        self,
        region: str = None,
        category: str = "1000",
        page: int = 1,
        limit: int = 50,
        callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Crawl danh sách các listings từ Chotot

        Args:
            region: ID khu vực (None để tìm toàn quốc)
            category: ID danh mục (mặc định: 1000 - Bất động sản)
            page: Số trang
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả từng item

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        url = (
            f"{self.api_base_url}?cg={category}&limit={limit}&o={(page-1)*limit}&st=s,k"
        )
        logger.info(f"Fetching listings from {url}")
        if region:
            url += f"&region={region}"

        results = {"total": 0, "successful": 0, "failed": 0}

        try:
            async with aiohttp.ClientSession() as session:
                response_data = await self.fetch_api(session, url)

                if not response_data:
                    logger.warning(f"No response data for page {page}")
                    return results

                # Trường hợp API trả về cấu trúc {total: x, ads: [...]}
                total_count = len(response_data.get("ads", []))
                listings = response_data.get("ads", [])

                if not listings:
                    logger.info(f"No listings found for page {page}")
                    return results

                results["total"] = total_count
                successful = 0
                failed = 0

                # Xử lý từng listing trực tiếp từ API response
                for ad_data in listings:
                    try:
                        # Tạo URL và listing_id
                        listing_id = str(ad_data.get("list_id", ""))
                        url = f"https://gateway.chotot.com/v1/public/ad-listing/{listing_id}"

                        # Kiểm tra checkpoint để tránh crawl lại
                        checkpoint = load_checkpoint(self.checkpoint_file)
                        if (
                            checkpoint
                            and listing_id in checkpoint
                            and checkpoint[listing_id]
                        ):
                            logger.info(
                                f"[Chotot] Post {listing_id} already crawled, skipping"
                            )
                            continue

                        # Làm sạch dữ liệu mô tả (loại bỏ HTML)
                        description = self._clean_html(ad_data.get("body", ""))

                        # Tạo thông tin người bán
                        seller_info = {
                            "name": ad_data.get("account_name", ""),
                            "account_id": str(ad_data.get("account_id", "")),
                            "post_time": ad_data.get("list_time", ""),
                        }

                        # Trích xuất thông tin location
                        location = self._extract_location_from_api(ad_data)

                        # Xây dựng đối tượng HouseDetailItem
                        house_detail = HouseDetailItem(
                            title=ad_data.get("subject", ""),
                            description=description,
                            price=str(ad_data.get("price", "")),
                            area=str(ad_data.get("size", "")),
                            bedroom=str(ad_data.get("rooms", "")),
                            bathroom=str(ad_data.get("toilets", "")),
                            house_direction=ad_data.get("direction", ""),
                            legal_status=ad_data.get("property_legal_document", ""),
                            latitude=str(ad_data.get("latitude", "")),
                            longitude=str(ad_data.get("longitude", "")),
                            floor_count="",
                            furnishing_sell=ad_data.get("furnishing_sell", ""),
                            seller_info=seller_info,
                            location=location,
                            price_per_m2=str(ad_data.get("price_million_per_m2", "")),
                            source="chotot",
                        )

                        # Chuyển đổi thành Dictionary
                        result = {}
                        for field_name, field_value in house_detail.__dict__.items():
                            # Bỏ qua các trường None
                            if field_value is not None:
                                result[field_name] = field_value

                        # Thêm các thông tin khác
                        result["url"] = url
                        result["listing_id"] = listing_id
                        result["timestamp"] = datetime.now().isoformat()

                        # Thêm các trường khác có thể hữu ích cho phân tích
                        if "ad_id" in ad_data:
                            result["ad_id"] = ad_data["ad_id"]
                        if "list_time" in ad_data:
                            result["created_time"] = ad_data["list_time"]

                        successful += 1
                        save_checkpoint(self.checkpoint_file, listing_id, success=True)
                        # gọi callback ngay lập tức
                        if callback:
                            await callback(result)

                    except Exception as e:
                        logger.error(
                            f"Error processing listing {ad_data.get('list_id', 'unknown')}: {e}"
                        )
                        failed += 1

                results["successful"] = successful
                results["failed"] = failed

                logger.info(
                    f"Page {page}: Processed {successful} listings successfully, {failed} failed"
                )
                return results

        except Exception as e:
            logger.error(f"Error crawling listings page {page}: {e}")
            return results

    async def crawl_range(
        self,
        start_page: int = 1,
        end_page: int = 5,
        region: str = None,
        category: str = "1000",
        limit: int = 50,
        callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Crawl nhiều trang listings từ Chotot

        Args:
            start_page: Trang bắt đầu
            end_page: Trang kết thúc
            region: ID khu vực
            category: ID danh mục
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả
            force: Bỏ qua checkpoint

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        total_results = {"total": 0, "successful": 0, "failed": 0}
        tasks = []

        # Tạo semaphore để giới hạn số lượng tasks chạy đồng thời
        sem = asyncio.Semaphore(self.max_concurrent)

        async def process_page(page_number):
            async with sem:
                # Thêm độ trễ ngẫu nhiên để tránh bị chặn
                delay = self.get_random_delay(1, 2)
                if page_number > start_page:
                    await asyncio.sleep(delay)

                logger.info(f"Crawling page {page_number}...")
                result = await self.crawl_listings(
                    region=region,
                    category=category,
                    page=page_number,
                    limit=limit,
                    callback=callback,
                )
                return result

        # Tạo task cho mỗi trang
        for page in range(start_page, end_page + 1):
            tasks.append(asyncio.create_task(process_page(page)))

        # Chạy tất cả các tasks và đợi kết quả
        results = await asyncio.gather(*tasks)

        # Tổng hợp kết quả
        for result in results:
            total_results["total"] += result["total"]
            total_results["successful"] += result["successful"]
            total_results["failed"] += result["failed"]

        logger.info(
            f"Range crawl summary: {end_page-start_page+1} pages, {total_results['total']} listings found, "
            f"{total_results['successful']} successful, {total_results['failed']} failed"
        )
        return total_results

    async def parse_response(
        self, response_data: Dict[str, Any], identifier: str
    ) -> Dict[str, Any]:
        """
        Phân tích dữ liệu API response và chuyển đổi sang HouseDetailItem

        Args:
            response_data: Dữ liệu từ API
            identifier: Định danh của item

        Returns:
            Dict: Dữ liệu đã được phân tích
        """
        try:
            # Kiểm tra dữ liệu có hợp lệ không
            if not response_data:
                raise ValueError(
                    f"Invalid API response for {identifier}: Empty response"
                )

            if isinstance(response_data, str):
                # Nếu response là string, thử parse JSON
                try:
                    response_data = json.loads(response_data)
                except:
                    raise ValueError(f"Invalid JSON response for {identifier}")

            # Tìm ad_data từ cấu trúc response
            ad_data = None

            # Trường hợp 1: "ads" là danh sách
            if "ads" in response_data and isinstance(response_data["ads"], list):
                # Lọc danh sách ads để tìm tin có ID khớp với identifier
                listing_id = self._extract_listing_id(identifier)
                for ad in response_data["ads"]:
                    if str(ad.get("list_id")) == listing_id:
                        ad_data = ad
                        break

                # Nếu không tìm thấy, dùng phần tử đầu tiên (nếu có)
                if ad_data is None and len(response_data["ads"]) > 0:
                    ad_data = response_data["ads"][0]

            # Trường hợp 2: "ad" là object
            elif "ad" in response_data:
                ad_data = response_data["ad"]

            # Nếu không tìm thấy ad_data
            if ad_data is None:
                raise ValueError(f"No ad data found for {identifier}")

            # Tạo URL cho tin đăng
            url = (
                identifier
                if identifier.startswith("http")
                else f"https://nha.chotot.com/{ad_data.get('list_id', '')}"
            )

            # Tạo thông tin người bán theo model mới
            seller_info = {
                "account_name": ad_data.get("account_name", ""),
                "account_id": str(ad_data.get("account_id", "")),
                "phone": ad_data.get("phone", ""),
                "post_time": ad_data.get("list_time", ""),
            }

            # Lấy thông tin từ properties nếu có
            rooms = ""
            toilets = ""
            direction = ""
            legal_document = ""

            if "properties" in ad_data and isinstance(ad_data["properties"], dict):
                props = ad_data["properties"]
                rooms = str(props.get("rooms", ""))
                toilets = str(props.get("toilets", ""))
                direction = props.get("direction", "")
                legal_document = props.get("legal_document", "")

            # Xây dựng đối tượng HouseDetailItem từ dữ liệu API theo model mới
            house_detail = HouseDetailItem(
                # Trường bắt buộc (cốt lõi)
                title=ad_data.get("subject", ""),
                description=ad_data.get("body", ""),
                price=str(ad_data.get("price_string", "")),
                area=str(ad_data.get("size", "")),
                price_per_m2=str(ad_data.get("price_million_per_m2", "")),
                bedroom=rooms,
                bathroom=toilets,
                legal_status=legal_document,
                latitude=str(ad_data.get("latitude", "")),
                longitude=str(ad_data.get("longitude", "")),
                # Trường tùy chọn (đặc thù của chotot)
                house_direction=direction,
                floor_count=str(ad_data.get("floors", "")),
                furnishing_sell=ad_data.get("furnishing_sell", ""),
                seller_info=seller_info,
                # Nguồn dữ liệu
                source="chotot",
            )

            # Chuyển đổi HouseDetailItem thành Dictionary để tiếp tục xử lý
            result = self._house_detail_to_dict(house_detail)

            # Thêm một số thông tin không có trong model
            result["url"] = url
            result["listing_id"] = str(ad_data.get("list_id", ""))
            result["crawl_timestamp"] = datetime.now().isoformat()

            return result

        except Exception as e:
            print(f"Error parsing response for {identifier}: {e}")
            raise
