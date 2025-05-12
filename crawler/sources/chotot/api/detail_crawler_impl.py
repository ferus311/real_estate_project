import asyncio
import json
import re
import aiohttp
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import os

from common.base.base_detail_crawler import BaseDetailCrawler
from common.utils.checkpoint import load_checkpoint, save_checkpoint
from common.models.house_detail import HouseDetailItem


class ChototDetailCrawler(BaseDetailCrawler):
    """
    Crawler chi tiết cho Chotot sử dụng API
    Chỉ tập trung vào nhiệm vụ crawl chi tiết từng tin đăng
    """

    def __init__(self, max_concurrent: int = 10):
        super().__init__(source="chotot", max_concurrent=max_concurrent)
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
        Đặc biệt tập trung vào việc chuẩn hóa dữ liệu cho mô hình HouseDetailItem

        Args:
            response_data: Dữ liệu từ API
            identifier: Định danh của item

        Returns:
            Dict: Dữ liệu đã được phân tích theo cấu trúc của HouseDetailItem
        """
        try:
            # Kiểm tra dữ liệu có hợp lệ không
            if not response_data:
                raise ValueError(
                    f"Invalid API response for {identifier}: Empty response"
                )

            # In ra để debug
            print(f"Response data type: {type(response_data)}")
            print(
                f"Response keys: {response_data.keys() if isinstance(response_data, dict) else 'Not a dict'}"
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

            print(
                f"Ad data keys: {ad_data.keys() if isinstance(ad_data, dict) else 'Not a dict'}"
            )

            # Tạo URL cho tin đăng
            url = (
                identifier
                if identifier.startswith("http")
                else f"https://nha.chotot.com/{ad_data.get('list_id', '')}"
            )

            # Tạo thông tin người bán theo model mới
            seller_info = {
                "account_name": ad_data.get("account_name", ""),
                "account_id": ad_data.get("account_id", ""),
                "phone": ad_data.get("phone", ""),
                "post_time": ad_data.get("list_time", ""),
            }

            # Xây dựng đối tượng HouseDetailItem từ dữ liệu API theo model mới
            house_detail = HouseDetailItem(
                # Trường bắt buộc (cốt lõi)
                title=ad_data.get("subject", ""),
                description=ad_data.get("body", ""),
                price=ad_data.get("price_string", ""),
                area=str(ad_data.get("size", "")),
                price_per_m2=str(ad_data.get("price_million_per_m2", "")),
                bedroom=str(ad_data.get("rooms", "")),
                bathroom=str(ad_data.get("toilets", "")),
                legal_status=ad_data.get("property_legal_document", ""),
                latitude=str(ad_data.get("latitude", "")),
                longitude=str(ad_data.get("longitude", "")),
                # Trường tùy chọn (đặc thù của chotot)
                house_direction=ad_data.get("direction", ""),
                floor_count=str(ad_data.get("floors", "")),
                furnishing_sell=ad_data.get("furnishing_sell", ""),
                seller_info=seller_info,
                # Nguồn dữ liệu
                source="chotot",
            )  # Chuyển đổi HouseDetailItem thành Dictionary để tiếp tục xử lý
            result = self._house_detail_to_dict(house_detail)

            # Thêm một số thông tin không có trong model HouseDetailItem nhưng cần thiết cho xử lý hạ nguồn
            result["url"] = url
            result["listing_id"] = str(ad_data.get("list_id", ""))
            result["crawl_timestamp"] = self._get_current_timestamp()

            # Đảm bảo các trường số được lưu dưới dạng chuỗi để phù hợp với model
            for field in [
                "price",
                "area",
                "price_per_m2",
                "bedroom",
                "bathroom",
                "latitude",
                "longitude",
                "floor_count",
            ]:
                if field in result and not isinstance(result[field], str):
                    result[field] = str(result[field])

            return result

        except Exception as e:
            print(f"Error parsing response for {identifier}: {e}")
            raise

    async def crawl_detail(self, url: str) -> Dict[str, Any]:
        """
        Crawl chi tiết từ một URL (chức năng chính của class này)
        Phương thức này là entry point cho việc crawl chi tiết

        Args:
            url: URL cần crawl (hoặc mã định danh)

        Returns:
            Dict: Dữ liệu chi tiết theo cấu trúc HouseDetailItem, hoặc None nếu có lỗi
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

    # Removed crawl_listings method to focus strictly on detail crawling functionality

    def _house_detail_to_dict(self, house_detail: HouseDetailItem) -> Dict[str, Any]:
        """
        Chuyển đổi đối tượng HouseDetailItem thành Dictionary
        Hàm tiện ích này đảm bảo dữ liệu được chuẩn hóa theo mô hình HouseDetailItem

        Args:
            house_detail: Đối tượng HouseDetailItem đã được điền dữ liệu

        Returns:
            Dict[str, Any]: Dictionary chứa dữ liệu từ HouseDetailItem, đã được chuẩn hóa
        """
        # Chuyển đổi dataclass thành dict
        result = {}
        for field_name, field_value in house_detail.__dict__.items():
            # Bỏ qua các trường None
            if field_value is not None:
                # Đảm bảo seller_info là dictionary
                if field_name == "seller_info" and not isinstance(field_value, dict):
                    result[field_name] = {}
                else:
                    result[field_name] = field_value

        return result
