import asyncio
import json
import re
import aiohttp
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import os

from common.base.base_api_crawler import BaseApiCrawler
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from common.models.house_detail import HouseDetailItem


class ChototApiCrawler(BaseApiCrawler):
    """
    Crawler API cho Chotot sử dụng API
    """

    def __init__(self, max_concurrent: int = 10):
        super().__init__(source="chotot", max_concurrent=max_concurrent)
        self.api_base_url = "https://gateway.chotot.com/v1/public/ad-listing"
        self.detail_api_url = "https://gateway.chotot.com/v1/public/ad-listing"
        self.max_retries = 3

        # Đảm bảo thư mục checkpoint tồn tại
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)

        # Cập nhật headers đặc thù cho Chotot
        self.headers = {
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://nha.chotot.com",
            "Referer": "https://nha.chotot.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _create_session(self):
        """
        Tạo và trả về một session aiohttp mới

        Returns:
            aiohttp.ClientSession: Session mới
        """
        return aiohttp.ClientSession()

    def _get_current_timestamp(self):
        """
        Trả về timestamp hiện tại dạng ISO

        Returns:
            str: Timestamp dạng ISO
        """
        return datetime.now().isoformat()

    def _extract_listing_id(self, identifier: str) -> str:
        """
        Trích xuất listing_id từ identifier (URL hoặc ID)

        Args:
            identifier: URL hoặc ID của listing

        Returns:
            str: ID của listing
        """
        # Nếu identifier là URL, trích xuất ID từ URL
        if identifier.startswith("http"):
            # Mẫu URL mới: https://nha.chotot.com/binh-duong/thuan-an/mua-ban-nha-dat/121532639.htm
            # Mẫu URL cũ: https://nha.chotot.com/xxx-xxx-xxx-xxx.12345678
            # Trích xuất ID từ phần cuối URL
            patterns = [
                r"/(\d+)\.htm$",  # Mẫu mới: /123456789.htm
                r"\.(\d+)(?:\?|$)",  # Mẫu cũ: .123456789
                r"/(\d+)(?:\?|$)",  # Mẫu khác: /123456789
            ]

            for pattern in patterns:
                match = re.search(pattern, identifier)
                if match:
                    return match.group(1)

            raise ValueError(f"URL format not recognized: {identifier}")

        # Nếu identifier là số, đó là ID
        elif identifier.isdigit():
            return identifier

        # Trường hợp không xác định được
        raise ValueError(f"Cannot extract listing ID from: {identifier}")

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
        return f"{self.detail_api_url}?list_id={listing_id}"

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
        retries = 3

        while retries > 0:
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = self.get_random_delay(2, 5)
                        print(f"Rate limited. Waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        retries -= 1
                    else:
                        print(f"API error: {response.status} for {url}")
                        retries -= 1
                        await asyncio.sleep(1)
            except Exception as e:
                print(f"Request error: {e} for {url}")
                retries -= 1
                await asyncio.sleep(1)

        return None

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

    async def crawl_detail(self, url: str) -> Dict[str, Any]:
        """
        Crawl chi tiết từ một URL

        Args:
            url: URL cần crawl

        Returns:
            Dict: Dữ liệu chi tiết, hoặc None nếu có lỗi
        """
        try:
            # Lấy ID từ URL để kiểm tra checkpoint
            listing_id = self._extract_listing_id(url)

            # Kiểm tra checkpoint để tránh crawl lại
            if listing_id:
                checkpoint = load_checkpoint(self.checkpoint_file)
                if checkpoint and listing_id in checkpoint and checkpoint[listing_id]:
                    print(f"[Chotot Detail] Post {listing_id} already crawled")
                    return {"skipped": True, "url": url}

            # Thực hiện crawl với số lần retry
            retries = self.max_retries
            while retries > 0:
                try:
                    async with aiohttp.ClientSession() as session:
                        api_url = await self.get_api_url(url)
                        print(f"[Chotot Detail] Crawling {url}")
                        print(f"[Chotot Detail] API URL: {api_url}")

                        response_data = await self.fetch_api(session, api_url)

                        if response_data:
                            result = await self.parse_response(response_data, url)

                            # Lưu checkpoint nếu thành công
                            if listing_id:
                                save_checkpoint(
                                    self.checkpoint_file, listing_id, success=True
                                )

                            return result

                    retries -= 1
                    await asyncio.sleep(self.get_random_delay())

                except Exception as e:
                    print(f"[Chotot Detail] Error during crawl: {e}")
                    retries -= 1
                    await asyncio.sleep(self.get_random_delay(1, 3))

            # Đánh dấu thất bại trong checkpoint
            if listing_id:
                save_checkpoint(self.checkpoint_file, listing_id, success=False)

            return None

        except Exception as e:
            print(f"[Chotot Detail] Fatal error crawling {url}: {e}")
            return None

    async def process_result(
        self, result: Dict[str, Any], callback: Optional[Callable] = None
    ) -> bool:
        """
        Xử lý kết quả crawl

        Args:
            result: Kết quả crawl
            callback: Hàm callback để xử lý kết quả

        Returns:
            bool: True nếu xử lý thành công, False nếu thất bại
        """
        try:
            # Nếu kết quả là None hoặc không có dữ liệu
            if not result:
                return False

            # Nếu đã skip, không cần xử lý
            if result.get("skipped"):
                return True

            # Gọi callback nếu có
            if callback and result:
                await callback(result)
                return True

            return False
        except Exception as e:
            print(f"[Chotot Detail] Error processing result: {e}")
            return False

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
        url = f"{self.api_base_url}?cg={category}&limit={limit}&o={(page-1)*limit}"
        if region:
            url += f"&region={region}"

        results = {"total": 0, "successful": 0, "failed": 0}

        try:
            async with aiohttp.ClientSession() as session:
                response_data = await self.fetch_api(session, url)

                if not response_data or "ads" not in response_data:
                    print(f"No listings found for page {page}")
                    return results

                listings = response_data.get("ads", [])
                listing_ids = [
                    str(ad.get("list_id")) for ad in listings if "list_id" in ad
                ]

                results["total"] = len(listing_ids)

                if listing_ids and callback:
                    # Crawl chi tiết và gọi callback cho từng listing
                    batch_results = await self.crawl_batch(listing_ids, callback)
                    results["successful"] = batch_results["successful"]
                    results["failed"] = batch_results["failed"]

                return results

        except Exception as e:
            print(f"Error crawling listings page {page}: {e}")
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

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        total_results = {"total": 0, "successful": 0, "failed": 0}
        tasks = []

        # Tạo semaphore để giới hạn số lượng tasks chạy đồng thời
        sem = asyncio.Semaphore(self.max_concurrent)

        async def process_page(page_number):
            async with sem:
                print(f"Crawling page {page_number}...")
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

        return total_results

    def _house_detail_to_dict(self, house_detail: HouseDetailItem) -> Dict[str, Any]:
        """
        Chuyển đổi đối tượng HouseDetailItem thành Dictionary

        Args:
            house_detail: Đối tượng HouseDetailItem

        Returns:
            Dict[str, Any]: Dictionary chứa dữ liệu từ HouseDetailItem
        """
        # Chuyển đổi dataclass thành dict
        result = {}
        for field_name, field_value in house_detail.__dict__.items():
            # Bỏ qua các trường None
            if field_value is not None:
                result[field_name] = field_value

        return result
