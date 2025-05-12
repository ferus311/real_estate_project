from typing import Dict, Type, Any, Optional
import logging
import os

from common.base.base_storage import BaseStorage
from common.storage.local_storage_impl import LocalStorage
from common.storage.hdfs_storage_impl import HDFSStorage

logger = logging.getLogger(__name__)


class StorageFactory:
    """
    Factory để tạo và quản lý các storage instances
    """

    # Registry cho các loại storage
    STORAGE_REGISTRY: Dict[str, Type[BaseStorage]] = {
        "local": LocalStorage,
        "hdfs": HDFSStorage,
        # Thêm các storage khác ở đây
    }

    @classmethod
    def create_storage(cls, storage_type: str = "local", **kwargs) -> BaseStorage:
        """
        Tạo một storage instance dựa trên loại

        Args:
            storage_type: Loại storage (local, hdfs, etc.)
            **kwargs: Tham số bổ sung cho storage

        Returns:
            BaseStorage: Instance của storage

        Raises:
            ValueError: Nếu không tìm thấy storage phù hợp
        """
        # Lấy storage type từ biến môi trường nếu không được chỉ định
        storage_type = storage_type or os.environ.get("STORAGE_TYPE", "local")

        if storage_type not in cls.STORAGE_REGISTRY:
            available_types = list(cls.STORAGE_REGISTRY.keys())
            logger.warning(
                f"Storage type '{storage_type}' not found, using 'local'. Available types: {available_types}"
            )
            storage_type = "local"

        storage_class = cls.STORAGE_REGISTRY[storage_type]

        # Lấy các tham số cấu hình từ biến môi trường nếu không được cung cấp
        if storage_type == "hdfs" and "namenode" not in kwargs:
            kwargs["namenode"] = os.environ.get("HDFS_NAMENODE", "namenode:9870")
            kwargs["user"] = os.environ.get("HDFS_USER", "airflow")
            kwargs["base_path"] = os.environ.get("HDFS_BASE_PATH", "/data/realestate")
        elif storage_type == "local" and "base_dir" not in kwargs:
            kwargs["base_dir"] = os.environ.get("LOCAL_STORAGE_PATH", "/data/local")

        logger.info(f"Creating storage of type '{storage_type}' with params: {kwargs}")
        return storage_class(**kwargs)

    @classmethod
    def register_storage(
        cls, storage_type: str, storage_class: Type[BaseStorage]
    ) -> None:
        """
        Đăng ký một storage mới

        Args:
            storage_type: Loại storage
            storage_class: Class của storage
        """
        cls.STORAGE_REGISTRY[storage_type] = storage_class
        logger.info(f"Registered storage: {storage_type}")

    @classmethod
    def get_available_storage_types(cls) -> Dict[str, str]:
        """
        Lấy danh sách các loại storage có sẵn

        Returns:
            Dict: Danh sách các loại storage và mô tả
        """
        return {
            "local": "Local file system storage",
            "hdfs": "Hadoop Distributed File System storage",
        }
