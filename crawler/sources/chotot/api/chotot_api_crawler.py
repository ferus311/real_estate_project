import asyncio
import json
import re
import aiohttp
import os
from typing import Dict, Any, Optional, List, Callable, Union
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

    def _extract_listing_id(self, identifier: Union[str, int]) -> str:
        """
        Trích xuất ID từ identifier (có thể là URL hoặc ID)

        Args:
            identifier: URL hoặc ID của tin đăng

        Returns:
            str: ID của tin đăng
        """
        if isinstance(identifier, int):
            return str(identifier)

        if isinstance(identifier, str):
            # Nếu là ID thuần túy
            if identifier.isdigit():
                return identifier

            # Nếu là URL, thử trích xuất ID
            match = re.search(r"\/(\d+)(?:\.htm)?$", identifier)
            if match:
                return match.group(1)

        # Nếu không xác định được, trả về nguyên chuỗi
        return str(identifier)

    def _house_detail_to_dict(self, house_detail: HouseDetailItem) -> Dict[str, Any]:
        """
        Chuyển đổi HouseDetailItem thành Dictionary

        Args:
            house_detail: Đối tượng HouseDetailItem

        Returns:
            Dict[str, Any]: Dictionary biểu diễn đối tượng
        """
        result = {}
        for field_name, field_value in house_detail.__dict__.items():
            # Bỏ qua các trường None
            if field_value is not None:
                result[field_name] = field_value
        return result

    def _extract_location_from_api(self, ad_data: Dict[str, Any]) -> str:
        """
        Trích xuất thông tin địa điểm từ dữ liệu API

        Args:
            ad_data: Dữ liệu API của một listing

        Returns:
            str: Thông tin địa điểm
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

    def _parse_ad_data(
        self, ad_data: Dict[str, Any], url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Phân tích dữ liệu của một tin đăng và chuyển đổi thành model

        Args:
            ad_data: Dữ liệu của tin đăng
            url: URL của tin đăng (nếu có)

        Returns:
            Dict[str, Any]: Dữ liệu đã được phân tích
        """
        try:
            # Lấy listing_id
            listing_id = str(ad_data.get("list_id", ""))

            # Làm sạch dữ liệu mô tả (loại bỏ HTML)
            description = self._clean_html(ad_data.get("body", ""))

            # Tạo URL cho tin đăng nếu chưa có
            if not url:
                url = f"https://nha.chotot.com/{listing_id}"

            # Tạo thông tin người bán
            seller_info = {
                "name": ad_data.get("account_name", ""),
                "account_id": str(ad_data.get("account_id", "")),
                "phone": ad_data.get("phone", ""),
                "post_time": ad_data.get("list_time", ""),
            }

            # Lấy thông tin từ properties nếu có
            rooms = str(ad_data.get("rooms", ""))
            toilets = str(ad_data.get("toilets", ""))
            direction = ad_data.get("direction", "")
            legal_document = ad_data.get("property_legal_document", "")

            if "properties" in ad_data and isinstance(ad_data["properties"], dict):
                props = ad_data["properties"]
                if not rooms and "rooms" in props:
                    rooms = str(props.get("rooms", ""))
                if not toilets and "toilets" in props:
                    toilets = str(props.get("toilets", ""))
                if not direction and "direction" in props:
                    direction = props.get("direction", "")
                if not legal_document and "legal_document" in props:
                    legal_document = props.get("legal_document", "")

            # Trích xuất thông tin location
            location = self._extract_location_from_api(ad_data)

            # Xây dựng đối tượng HouseDetailItem từ dữ liệu API theo model mới
            house_detail = HouseDetailItem(
                # Trường bắt buộc (cốt lõi)
                title=ad_data.get("subject", ""),
                description=description,
                price=str(ad_data.get("price_string", ad_data.get("price", ""))),
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
                location=location,
                # Nguồn dữ liệu
                source="chotot",
            )

            # Chuyển đổi HouseDetailItem thành Dictionary
            result = self._house_detail_to_dict(house_detail)

            # Thêm một số thông tin không có trong model
            result["url"] = url
            result["listing_id"] = listing_id
            result["crawl_timestamp"] = datetime.now().isoformat()

            return result

        except Exception as e:
            logger.error(
                f"Error parsing ad_data for listing {ad_data.get('list_id', 'unknown')}: {e}"
            )
            raise

    async def crawl_listings(
        self,
        page: int = 1,
        limit: int = 50,
        callback: Optional[Callable] = None,
        region: str = None,
        category: str = "1000",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Crawl danh sách các listings từ Chotot

        Args:
            page: Số trang
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả từng item
            region: ID khu vực (None để tìm toàn quốc)
            category: ID danh mục (mặc định: 1000 - Bất động sản)
            **kwargs: Các tham số bổ sung khác

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
                        # Lấy listing_id
                        listing_id = str(ad_data.get("list_id", ""))

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

                        # Sử dụng phương thức _parse_ad_data thay vì parse_response
                        result = self._parse_ad_data(ad_data)

                        # Lưu checkpoint
                        save_checkpoint(self.checkpoint_file, listing_id, success=True)

                        # Gọi callback
                        if callback:
                            await callback(result)

                        successful += 1

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
        limit: int = 50,
        callback: Optional[Callable] = None,
        region: str = None,
        category: str = "1000",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Crawl nhiều trang listings từ Chotot

        Args:
            start_page: Trang bắt đầu
            end_page: Trang kết thúc
            limit: Số lượng kết quả mỗi trang
            callback: Hàm callback để xử lý kết quả
            region: ID khu vực
            category: ID danh mục
            **kwargs: Các tham số bổ sung khác

        Returns:
            Dict[str, Any]: Thống kê kết quả crawl
        """
        # Gọi phương thức crawl_range từ lớp cha với tham số đặc thù
        return await super().crawl_range(
            start_page=start_page,
            end_page=end_page,
            limit=limit,
            callback=callback,
            region=region,
            category=category,
            **kwargs,
        )

    async def crawl_item(self, identifier: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Crawl một item qua API (ghi đè phương thức của lớp cha)

        Args:
            identifier: Định danh của item (URL hoặc ID)

        Returns:
            Dict: Dữ liệu item, hoặc None nếu có lỗi
        """
        error_type = None
        try:
            # Validate identifier trước khi xử lý
            if not identifier:
                logger.error(f"Invalid identifier: empty value")
                error_type = "invalid_identifier"
                return None

            try:
                api_url = await self.get_api_url(identifier)
            except (ValueError, AttributeError) as e:
                logger.error(f"Failed to construct API URL for {identifier}: {e}")
                error_type = "invalid_url_format"
                return None

            async with aiohttp.ClientSession() as session:
                response_data = await self.fetch_api(session, api_url)

                if not response_data:
                    logger.warning(f"No data returned for identifier: {identifier}")
                    error_type = "empty_response"
                    return None

                if "ads" not in response_data or not response_data["ads"]:
                    logger.warning(
                        f"No ads data in response for identifier: {identifier}"
                    )
                    error_type = "missing_content"
                    # Return empty result with status instead of None for better tracking
                    return {
                        "source": self.source,
                        "crawl_timestamp": datetime.now().isoformat(),
                        "identifier": str(identifier),
                        "status": "no_content",
                        "skipped": True,
                    }

                result = await self.parse_response(response_data, identifier)
                result.update(
                    {
                        "source": self.source,
                        "crawl_timestamp": datetime.now().isoformat(),
                        "identifier": str(identifier),
                        "status": "success",
                    }
                )
                return result

        except aiohttp.ClientError as e:
            logger.error(f"Network error when crawling {identifier}: {e}")
            error_type = "network_error"
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for {identifier}: {e}")
            error_type = "invalid_response"
        except Exception as e:
            logger.error(f"Unexpected error crawling {identifier}: {e}")
            error_type = "unknown_error"

        # Return status information for better tracking
        return {
            "source": self.source,
            "crawl_timestamp": datetime.now().isoformat(),
            "identifier": str(identifier),
            "status": "error",
            "error_type": error_type,
            "skipped": True,
        }
