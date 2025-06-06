#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Model Registry để quản lý model versioning và metadata
"""

import os
import json
import shutil
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
from pathlib import Path
import pickle

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ModelRegistry:
    """
    Registry để quản lý các versions của models
    """

    def __init__(
        self, registry_path: str = "/home/fer/data/real_estate_project/models"
    ):
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)

        # Metadata file
        self.metadata_file = self.registry_path / "registry_metadata.json"
        self.load_metadata()

    def load_metadata(self):
        """Load registry metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, "r") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {
                "models": {},
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
            }

    def save_metadata(self):
        """Save registry metadata"""
        self.metadata["last_updated"] = datetime.now().isoformat()
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)

    def register_model(
        self,
        model_name: str,
        model_path: str,
        model_type: str,
        metrics: Dict[str, float],
        hyperparameters: Dict[str, Any],
        training_data_info: Dict[str, Any],
        feature_columns: List[str],
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> str:
        """
        Đăng ký một model version mới

        Args:
            model_name: Tên model
            model_path: Đường dẫn đến model đã train
            model_type: Loại model (price_prediction, etc.)
            metrics: Dict metrics đánh giá
            hyperparameters: Dict hyperparameters
            training_data_info: Thông tin về dữ liệu training
            feature_columns: Danh sách feature columns
            tags: Tags để phân loại model
            description: Mô tả model

        Returns:
            str: Version ID của model đã đăng ký
        """
        current_time = datetime.now()
        version_id = f"{model_name}_v{current_time.strftime('%Y%m%d_%H%M%S')}"

        # Tạo thư mục cho model version
        version_path = self.registry_path / model_name / version_id
        version_path.mkdir(parents=True, exist_ok=True)

        # Copy model files
        if os.path.exists(model_path):
            dest_model_path = version_path / "model"
            if os.path.isdir(model_path):
                shutil.copytree(model_path, dest_model_path, dirs_exist_ok=True)
            else:
                shutil.copy2(model_path, dest_model_path)

        # Tạo model metadata
        model_metadata = {
            "model_name": model_name,
            "version_id": version_id,
            "model_type": model_type,
            "created_at": current_time.isoformat(),
            "metrics": metrics,
            "hyperparameters": hyperparameters,
            "training_data_info": training_data_info,
            "feature_columns": feature_columns,
            "tags": tags or [],
            "description": description or "",
            "model_path": str(dest_model_path),
            "status": "registered",
        }

        # Save model metadata
        metadata_path = version_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(model_metadata, f, indent=2)

        # Update registry metadata
        if model_name not in self.metadata["models"]:
            self.metadata["models"][model_name] = {
                "versions": [],
                "latest_version": None,
                "production_version": None,
            }

        self.metadata["models"][model_name]["versions"].append(
            {
                "version_id": version_id,
                "created_at": current_time.isoformat(),
                "metrics": metrics,
                "status": "registered",
            }
        )

        # Set as latest version
        self.metadata["models"][model_name]["latest_version"] = version_id

        self.save_metadata()

        logger.info(f"✅ Model registered: {version_id}")
        logger.info(f"📍 Path: {version_path}")
        logger.info(f"📊 Metrics: {metrics}")

        return version_id

    def promote_to_production(self, model_name: str, version_id: str) -> bool:
        """
        Promote một model version lên production

        Args:
            model_name: Tên model
            version_id: Version ID

        Returns:
            bool: True nếu thành công
        """
        if model_name not in self.metadata["models"]:
            logger.error(f"Model {model_name} not found")
            return False

        # Kiểm tra version exists
        versions = [
            v["version_id"] for v in self.metadata["models"][model_name]["versions"]
        ]
        if version_id not in versions:
            logger.error(f"Version {version_id} not found for model {model_name}")
            return False

        # Update production version
        old_production = self.metadata["models"][model_name].get("production_version")
        self.metadata["models"][model_name]["production_version"] = version_id

        # Update version status
        for version in self.metadata["models"][model_name]["versions"]:
            if version["version_id"] == version_id:
                version["status"] = "production"
                version["promoted_at"] = datetime.now().isoformat()
            elif version.get("status") == "production":
                version["status"] = "registered"

        self.save_metadata()

        logger.info(f"✅ Model {version_id} promoted to production")
        if old_production:
            logger.info(f"📉 Previous production version: {old_production}")

        return True

    def get_model_info(
        self, model_name: str, version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Lấy thông tin model

        Args:
            model_name: Tên model
            version_id: Version ID (nếu None thì lấy latest)

        Returns:
            Dict thông tin model
        """
        if model_name not in self.metadata["models"]:
            raise ValueError(f"Model {model_name} not found")

        if version_id is None:
            version_id = self.metadata["models"][model_name]["latest_version"]

        version_path = self.registry_path / model_name / version_id / "metadata.json"
        if not version_path.exists():
            raise ValueError(f"Version {version_id} metadata not found")

        with open(version_path, "r") as f:
            return json.load(f)

    def get_production_model(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Lấy thông tin production model

        Args:
            model_name: Tên model

        Returns:
            Dict thông tin production model hoặc None
        """
        if model_name not in self.metadata["models"]:
            return None

        production_version = self.metadata["models"][model_name].get(
            "production_version"
        )
        if not production_version:
            return None

        return self.get_model_info(model_name, production_version)

    def list_models(self) -> Dict[str, Any]:
        """
        Liệt kê tất cả models

        Returns:
            Dict thông tin tất cả models
        """
        return self.metadata["models"]

    def list_model_versions(self, model_name: str) -> List[Dict[str, Any]]:
        """
        Liệt kê tất cả versions của một model

        Args:
            model_name: Tên model

        Returns:
            List versions
        """
        if model_name not in self.metadata["models"]:
            return []

        return self.metadata["models"][model_name]["versions"]

    def compare_models(
        self, model_name: str, version_ids: List[str], metric: str = "rmse"
    ) -> Dict[str, Any]:
        """
        So sánh các model versions

        Args:
            model_name: Tên model
            version_ids: Danh sách version IDs
            metric: Metric để so sánh

        Returns:
            Dict kết quả so sánh
        """
        comparison = {
            "metric": metric,
            "models": [],
            "best_version": None,
            "best_value": None,
        }

        for version_id in version_ids:
            try:
                model_info = self.get_model_info(model_name, version_id)
                metric_value = model_info["metrics"].get(metric)

                comparison["models"].append(
                    {
                        "version_id": version_id,
                        "metric_value": metric_value,
                        "created_at": model_info["created_at"],
                        "status": model_info["status"],
                    }
                )

                # Determine best (assuming lower is better for RMSE, MAE)
                if metric_value is not None:
                    if (
                        comparison["best_value"] is None
                        or metric_value < comparison["best_value"]
                    ):
                        comparison["best_version"] = version_id
                        comparison["best_value"] = metric_value

            except Exception as e:
                logger.warning(f"Could not get info for {version_id}: {str(e)}")

        return comparison

    def archive_model(self, model_name: str, version_id: str) -> bool:
        """
        Archive một model version

        Args:
            model_name: Tên model
            version_id: Version ID

        Returns:
            bool: True nếu thành công
        """
        try:
            # Update status in metadata
            for version in self.metadata["models"][model_name]["versions"]:
                if version["version_id"] == version_id:
                    version["status"] = "archived"
                    version["archived_at"] = datetime.now().isoformat()
                    break

            self.save_metadata()
            logger.info(f"✅ Model {version_id} archived")
            return True

        except Exception as e:
            logger.error(f"Failed to archive model {version_id}: {str(e)}")
            return False

    def cleanup_old_versions(self, model_name: str, keep_latest: int = 5) -> int:
        """
        Cleanup old model versions, chỉ giữ lại N versions mới nhất

        Args:
            model_name: Tên model
            keep_latest: Số versions mới nhất cần giữ

        Returns:
            int: Số versions đã xóa
        """
        if model_name not in self.metadata["models"]:
            return 0

        versions = self.metadata["models"][model_name]["versions"]

        # Sort by created_at (newest first)
        versions.sort(key=lambda x: x["created_at"], reverse=True)

        # Get production version to protect it
        production_version = self.metadata["models"][model_name].get(
            "production_version"
        )

        deleted_count = 0
        for i, version in enumerate(versions):
            # Keep latest N versions and production version
            if i >= keep_latest and version["version_id"] != production_version:
                version_path = self.registry_path / model_name / version["version_id"]
                if version_path.exists():
                    shutil.rmtree(version_path)
                    deleted_count += 1
                    logger.info(f"🗑️ Deleted old version: {version['version_id']}")

        # Update metadata (remove deleted versions)
        self.metadata["models"][model_name]["versions"] = [
            v
            for i, v in enumerate(versions)
            if i < keep_latest or v["version_id"] == production_version
        ]

        self.save_metadata()

        return deleted_count


def main():
    """Test function"""
    registry = ModelRegistry()

    # Example usage
    print("Available models:", registry.list_models())


if __name__ == "__main__":
    main()
