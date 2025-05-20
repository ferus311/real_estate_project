from typing import Dict, Type, Optional, Any
import logging
import os

from common.base.base_list_crawler import BaseListCrawler
from common.base.base_detail_crawler import BaseDetailCrawler
from common.base.base_api_crawler import BaseApiCrawler

# Import các crawler cụ thể
from sources.batdongsan.playwright.list_crawler_impl import (
    BatdongsanListCrawler,
)
from sources.batdongsan.playwright.detail_crawler_impl import BatdongsanDetailCrawler
from sources.chotot.api.detail_crawler_impl import ChototDetailCrawler
from sources.chotot.api.api_crawler_impl import ChototApiCrawler

logger = logging.getLogger(__name__)


class CrawlerFactory:
    """
    Factory để tạo và quản lý các crawler instances
    """

    # Registry cho list crawler
    LIST_CRAWLER_REGISTRY: Dict[str, Dict[str, Type[BaseListCrawler]]] = {
        "batdongsan": {
            "default": BatdongsanListCrawler,
            "playwright": BatdongsanListCrawler,
            # Thêm các crawler khác cho batdongsan ở đây
        }
        # Thêm các source khác ở đây
    }

    # Registry cho detail crawler
    DETAIL_CRAWLER_REGISTRY: Dict[str, Dict[str, Type[BaseDetailCrawler]]] = {
        "default": {
            "default": BatdongsanDetailCrawler,
        },
        "batdongsan": {
            "default": BatdongsanDetailCrawler,
        },
        "chotot": {
            "default": ChototDetailCrawler,
        },
        # Thêm các source khác ở đây
    }

    # Registry cho API crawler
    API_CRAWLER_REGISTRY: Dict[str, Dict[str, Type[BaseApiCrawler]]] = {
        "default": {
            "default": ChototApiCrawler,
        },
        "chotot": {
            "default": ChototApiCrawler,
        },
        # Thêm các source khác ở đây
    }

    @classmethod
    def create_list_crawler(
        cls, source: str, crawler_type: str = "default", **kwargs
    ) -> BaseListCrawler:
        """
        Tạo một list crawler dựa trên source và crawler_type

        Args:
            source: Nguồn dữ liệu (batdongsan, chotot, etc.)
            crawler_type: Loại crawler (playwright, selenium, etc.)
            **kwargs: Tham số bổ sung cho crawler

        Returns:
            BaseListCrawler: Instance của crawler

        Raises:
            ValueError: Nếu không tìm thấy crawler phù hợp
        """
        # Lấy source và crawler_type từ biến môi trường nếu không được chỉ định
        source = source or os.environ.get("SOURCE", "batdongsan")
        crawler_type = crawler_type or os.environ.get("CRAWLER_TYPE", "playwright")

        if source not in cls.LIST_CRAWLER_REGISTRY:
            raise ValueError(f" Unsupported source: {source}")

        available_types = cls.LIST_CRAWLER_REGISTRY[source]

        if crawler_type not in available_types:
            # Fallback to default if available
            if "default" in available_types:
                crawler_type = "default"
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using default"
                )
            else:
                # Use first available type
                crawler_type = next(iter(available_types.keys()))
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using {crawler_type}"
                )

        crawler_class = cls.LIST_CRAWLER_REGISTRY[source][crawler_type]
        return crawler_class(**kwargs)

    @classmethod
    def create_detail_crawler(
        cls, source: str, crawler_type: str = "default", **kwargs
    ) -> BaseDetailCrawler:
        """
        Tạo một detail crawler dựa trên source và crawler_type

        Args:
            source: Nguồn dữ liệu (batdongsan, chotot, etc.)
            crawler_type: Loại crawler (default, etc.)
            **kwargs: Tham số bổ sung cho crawler

        Returns:
            BaseDetailCrawler: Instance của crawler

        Raises:
            ValueError: Nếu không tìm thấy crawler phù hợp
        """
        # Lấy source và crawler_type từ biến môi trường nếu không được chỉ định
        source = source or os.environ.get("SOURCE", "batdongsan")
        crawler_type = crawler_type or os.environ.get("CRAWLER_TYPE", "default")

        if source not in cls.DETAIL_CRAWLER_REGISTRY:
            raise ValueError(f"Unsupported source: {source}")

        available_types = cls.DETAIL_CRAWLER_REGISTRY[source]

        if crawler_type not in available_types:
            # Fallback to default if available
            if "default" in available_types:
                crawler_type = "default"
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using default"
                )
            else:
                # Use first available type
                crawler_type = next(iter(available_types.keys()))
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using {crawler_type}"
                )

        crawler_class = cls.DETAIL_CRAWLER_REGISTRY[source][crawler_type]
        return crawler_class(**kwargs)

    @classmethod
    def register_list_crawler(
        cls, source: str, crawler_type: str, crawler_class: Type[BaseListCrawler]
    ) -> None:
        """
        Đăng ký một list crawler mới

        Args:
            source: Nguồn dữ liệu
            crawler_type: Loại crawler
            crawler_class: Class của crawler
        """
        if source not in cls.LIST_CRAWLER_REGISTRY:
            cls.LIST_CRAWLER_REGISTRY[source] = {}

        cls.LIST_CRAWLER_REGISTRY[source][crawler_type] = crawler_class
        logger.info(f"Registered list crawler: {source}.{crawler_type}")

    @classmethod
    def register_detail_crawler(
        cls, source: str, crawler_type: str, crawler_class: Type[BaseDetailCrawler]
    ) -> None:
        """
        Đăng ký một detail crawler mới

        Args:
            source: Nguồn dữ liệu
            crawler_type: Loại crawler
            crawler_class: Class của crawler
        """
        if source not in cls.DETAIL_CRAWLER_REGISTRY:
            cls.DETAIL_CRAWLER_REGISTRY[source] = {}

        cls.DETAIL_CRAWLER_REGISTRY[source][crawler_type] = crawler_class
        logger.info(f"Registered detail crawler: {source}.{crawler_type}")

    @classmethod
    def create_api_crawler(
        cls, source: str, crawler_type: str = "default", **kwargs
    ) -> BaseApiCrawler:
        """
        Tạo một API crawler dựa trên source và crawler_type

        Args:
            source: Nguồn dữ liệu (chotot, etc.)
            crawler_type: Loại crawler (default, etc.)
            **kwargs: Tham số bổ sung cho crawler

        Returns:
            BaseApiCrawler: Instance của crawler

        Raises:
            ValueError: Nếu không tìm thấy crawler phù hợp
        """
        # Lấy source từ biến môi trường nếu không được chỉ định
        source = source or os.environ.get("SOURCE", "chotot")
        crawler_type = crawler_type or os.environ.get("CRAWLER_TYPE", "default")

        if source not in cls.API_CRAWLER_REGISTRY:
            raise ValueError(f"Unsupported source for API crawler: {source}")

        available_types = cls.API_CRAWLER_REGISTRY[source]

        if crawler_type not in available_types:
            # Fallback to default if available
            if "default" in available_types:
                crawler_type = "default"
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using default"
                )
            else:
                # Use first available type
                crawler_type = next(iter(available_types.keys()))
                logger.warning(
                    f"Crawler type '{crawler_type}' not found for source '{source}', using {crawler_type}"
                )

        crawler_class = cls.API_CRAWLER_REGISTRY[source][crawler_type]
        return crawler_class(**kwargs)

    @classmethod
    def register_api_crawler(
        cls, source: str, crawler_type: str, crawler_class: Type[BaseApiCrawler]
    ) -> None:
        """
        Đăng ký một API crawler mới

        Args:
            source: Nguồn dữ liệu
            crawler_type: Loại crawler
            crawler_class: Class của crawler
        """
        if source not in cls.API_CRAWLER_REGISTRY:
            cls.API_CRAWLER_REGISTRY[source] = {}

        cls.API_CRAWLER_REGISTRY[source][crawler_type] = crawler_class
        logger.info(f"Registered API crawler: {source}.{crawler_type}")

    @classmethod
    def get_available_sources(cls) -> Dict[str, Dict[str, Any]]:
        """
        Lấy danh sách các nguồn và crawler có sẵn

        Returns:
            Dict: Danh sách các nguồn và crawler
        """
        result = {}

        for source, types in cls.LIST_CRAWLER_REGISTRY.items():
            if source not in result:
                result[source] = {
                    "list_crawlers": [],
                    "detail_crawlers": [],
                    "api_crawlers": [],
                }
            result[source]["list_crawlers"] = list(types.keys())

        for source, types in cls.DETAIL_CRAWLER_REGISTRY.items():
            if source not in result:
                result[source] = {
                    "list_crawlers": [],
                    "detail_crawlers": [],
                    "api_crawlers": [],
                }
            result[source]["detail_crawlers"] = list(types.keys())

        for source, types in cls.API_CRAWLER_REGISTRY.items():
            if source not in result:
                result[source] = {
                    "list_crawlers": [],
                    "detail_crawlers": [],
                    "api_crawlers": [],
                }
            result[source]["api_crawlers"] = list(types.keys())

        return result
