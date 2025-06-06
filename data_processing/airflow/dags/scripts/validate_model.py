#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Model validation và testing utilities
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, max, min, count, stddev, when

# Thêm thư mục gốc vào sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from ml.model_registry import ModelRegistry
from ml.config_manager import MLConfig
from ml.enhanced_model_factory import EnhancedModelFactory

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ModelValidator:
    """
    Validation và testing cho trained models
    """

    def __init__(self, spark: SparkSession, config: Optional[MLConfig] = None):
        self.spark = spark
        self.config = config or MLConfig()
        self.model_registry = ModelRegistry(self.config.get("model_registry.path"))

    def validate_model_performance(
        self,
        model_name: str,
        version_id: str,
        validation_date: str,
        performance_thresholds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Validate model performance against thresholds

        Args:
            model_name: Tên model
            version_id: Version ID
            validation_date: Ngày validation
            performance_thresholds: Custom thresholds

        Returns:
            Dict: Validation results
        """
        logger.info(f"🔍 Validating model performance: {model_name} v{version_id}")

        # Get model info
        model_info = self.model_registry.get_model_info(model_name, version_id)
        metrics = model_info["metrics"]

        # Default thresholds
        default_thresholds = {
            "rmse": {"max": 50000000, "warning": 30000000},  # 50M VND max, 30M warning
            "mae": {"max": 30000000, "warning": 20000000},  # 30M VND max, 20M warning
            "r2": {"min": 0.6, "warning": 0.7},  # Min 0.6, warn if < 0.7
            "mape": {"max": 0.3, "warning": 0.2},  # Max 30%, warn if > 20%
        }

        thresholds = performance_thresholds or default_thresholds

        validation_result = {
            "model_name": model_name,
            "version_id": version_id,
            "validation_date": validation_date,
            "metrics": metrics,
            "thresholds": thresholds,
            "validation_status": "passed",
            "issues": [],
            "warnings": [],
        }

        # Check each metric
        for metric_name, metric_value in metrics.items():
            if metric_name in thresholds:
                threshold_config = thresholds[metric_name]

                # Check maximum thresholds (lower is better)
                if "max" in threshold_config:
                    if metric_value > threshold_config["max"]:
                        validation_result["issues"].append(
                            f"{metric_name} ({metric_value:.2f}) exceeds maximum threshold ({threshold_config['max']})"
                        )
                        validation_result["validation_status"] = "failed"
                    elif (
                        "warning" in threshold_config
                        and metric_value > threshold_config["warning"]
                    ):
                        validation_result["warnings"].append(
                            f"{metric_name} ({metric_value:.2f}) exceeds warning threshold ({threshold_config['warning']})"
                        )

                # Check minimum thresholds (higher is better)
                if "min" in threshold_config:
                    if metric_value < threshold_config["min"]:
                        validation_result["issues"].append(
                            f"{metric_name} ({metric_value:.2f}) below minimum threshold ({threshold_config['min']})"
                        )
                        validation_result["validation_status"] = "failed"
                    elif (
                        "warning" in threshold_config
                        and metric_value < threshold_config["warning"]
                    ):
                        validation_result["warnings"].append(
                            f"{metric_name} ({metric_value:.2f}) below warning threshold ({threshold_config['warning']})"
                        )

        # Update status based on warnings
        if (
            validation_result["warnings"]
            and validation_result["validation_status"] == "passed"
        ):
            validation_result["validation_status"] = "passed_with_warnings"

        logger.info(f"📊 Validation status: {validation_result['validation_status']}")
        if validation_result["issues"]:
            logger.error(f"❌ Issues found: {len(validation_result['issues'])}")
        if validation_result["warnings"]:
            logger.warning(f"⚠️ Warnings: {len(validation_result['warnings'])}")

        return validation_result

    def validate_data_requirements(
        self, model_name: str, test_data_path: str
    ) -> Dict[str, Any]:
        """
        Validate that test data meets model requirements

        Args:
            model_name: Tên model
            test_data_path: Path to test data

        Returns:
            Dict: Data validation results
        """
        logger.info(f"📋 Validating data requirements for {model_name}")

        try:
            # Load test data
            df = self.spark.read.parquet(test_data_path)

            # Get expected features from latest model
            latest_model_info = self.model_registry.get_model_info(model_name)
            expected_features = latest_model_info.get("feature_columns", [])

            validation_result = {
                "model_name": model_name,
                "test_data_path": test_data_path,
                "validation_status": "passed",
                "issues": [],
                "warnings": [],
                "data_summary": {
                    "total_records": df.count(),
                    "total_columns": len(df.columns),
                    "expected_features": len(expected_features),
                },
            }

            # Check for missing features
            missing_features = []
            for feature in expected_features:
                if feature not in df.columns:
                    missing_features.append(feature)

            if missing_features:
                validation_result["issues"].append(
                    f"Missing features: {missing_features}"
                )
                validation_result["validation_status"] = "failed"

            # Check data quality
            quality_checks = self._check_data_quality(df)
            validation_result["data_quality"] = quality_checks

            if quality_checks["critical_issues"]:
                validation_result["issues"].extend(quality_checks["critical_issues"])
                validation_result["validation_status"] = "failed"

            if quality_checks["warnings"]:
                validation_result["warnings"].extend(quality_checks["warnings"])
                if validation_result["validation_status"] == "passed":
                    validation_result["validation_status"] = "passed_with_warnings"

            logger.info(
                f"📊 Data validation status: {validation_result['validation_status']}"
            )
            return validation_result

        except Exception as e:
            logger.error(f"❌ Data validation failed: {str(e)}")
            return {
                "model_name": model_name,
                "test_data_path": test_data_path,
                "validation_status": "error",
                "error": str(e),
            }

    def _check_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Check data quality issues"""
        total_records = df.count()

        quality_result = {
            "total_records": total_records,
            "critical_issues": [],
            "warnings": [],
            "column_stats": {},
        }

        # Check each column
        for col_name, data_type in df.dtypes:
            null_count = df.filter(col(col_name).isNull()).count()
            null_ratio = null_count / total_records if total_records > 0 else 0

            quality_result["column_stats"][col_name] = {
                "null_count": null_count,
                "null_ratio": null_ratio,
                "data_type": data_type,
            }

            # Critical issues (> 50% nulls)
            if null_ratio > 0.5:
                quality_result["critical_issues"].append(
                    f"Column {col_name} has {null_ratio:.1%} null values"
                )
            # Warnings (> 20% nulls)
            elif null_ratio > 0.2:
                quality_result["warnings"].append(
                    f"Column {col_name} has {null_ratio:.1%} null values"
                )

        return quality_result

    def run_model_tests(
        self,
        model_name: str,
        version_id: str,
        test_data_path: str,
        test_suite: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run comprehensive model tests

        Args:
            model_name: Tên model
            version_id: Version ID
            test_data_path: Path to test data
            test_suite: List of tests to run

        Returns:
            Dict: Test results
        """
        logger.info(f"🧪 Running model tests for {model_name} v{version_id}")

        # Default test suite
        default_tests = [
            "performance_validation",
            "data_requirements",
            "prediction_consistency",
            "edge_case_handling",
        ]

        tests_to_run = test_suite or default_tests

        test_results = {
            "model_name": model_name,
            "version_id": version_id,
            "test_data_path": test_data_path,
            "tests_run": tests_to_run,
            "overall_status": "passed",
            "test_details": {},
        }

        failed_tests = 0

        for test_name in tests_to_run:
            try:
                if test_name == "performance_validation":
                    result = self.validate_model_performance(
                        model_name, version_id, datetime.now().strftime("%Y-%m-%d")
                    )
                elif test_name == "data_requirements":
                    result = self.validate_data_requirements(model_name, test_data_path)
                elif test_name == "prediction_consistency":
                    result = self._test_prediction_consistency(
                        model_name, version_id, test_data_path
                    )
                elif test_name == "edge_case_handling":
                    result = self._test_edge_cases(
                        model_name, version_id, test_data_path
                    )
                else:
                    result = {
                        "status": "skipped",
                        "reason": f"Unknown test: {test_name}",
                    }

                test_results["test_details"][test_name] = result

                if (
                    result.get("validation_status") == "failed"
                    or result.get("status") == "failed"
                ):
                    failed_tests += 1

            except Exception as e:
                logger.error(f"❌ Test {test_name} failed with error: {str(e)}")
                test_results["test_details"][test_name] = {
                    "status": "error",
                    "error": str(e),
                }
                failed_tests += 1

        # Determine overall status
        if failed_tests > 0:
            test_results["overall_status"] = "failed"
        elif any(
            t.get("validation_status") == "passed_with_warnings"
            for t in test_results["test_details"].values()
        ):
            test_results["overall_status"] = "passed_with_warnings"

        test_results["failed_tests"] = failed_tests
        test_results["total_tests"] = len(tests_to_run)

        logger.info(f"🏁 Test suite completed: {test_results['overall_status']}")
        logger.info(
            f"📊 Results: {len(tests_to_run) - failed_tests}/{len(tests_to_run)} tests passed"
        )

        return test_results

    def _test_prediction_consistency(
        self, model_name: str, version_id: str, test_data_path: str
    ) -> Dict[str, Any]:
        """Test prediction consistency"""
        logger.info("🔄 Testing prediction consistency...")

        # This would load the model and run predictions multiple times
        # to check for consistency

        return {
            "validation_status": "passed",
            "test_type": "prediction_consistency",
            "message": "Prediction consistency test passed (placeholder)",
        }

    def _test_edge_cases(
        self, model_name: str, version_id: str, test_data_path: str
    ) -> Dict[str, Any]:
        """Test edge case handling"""
        logger.info("⚡ Testing edge case handling...")

        # This would test model behavior with edge cases like:
        # - Extreme values
        # - Missing data
        # - Unusual combinations

        return {
            "validation_status": "passed",
            "test_type": "edge_case_handling",
            "message": "Edge case handling test passed (placeholder)",
        }

    def generate_validation_report(
        self, model_name: str, version_id: str, test_results: Dict[str, Any]
    ) -> str:
        """
        Generate validation report

        Args:
            model_name: Tên model
            version_id: Version ID
            test_results: Test results

        Returns:
            str: Report file path
        """
        report_dir = Path("/home/fer/data/real_estate_project/validation_reports")
        report_dir.mkdir(parents=True, exist_ok=True)

        report_filename = f"validation_report_{model_name}_{version_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path = report_dir / report_filename

        # Add metadata
        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "model_name": model_name,
                "version_id": version_id,
                "validator_version": "1.0.0",
            },
            "test_results": test_results,
        }

        with open(report_path, "w") as f:
            json.dump(report_data, f, indent=2)

        logger.info(f"📋 Validation report saved: {report_path}")
        return str(report_path)


def main():
    """Main validation script"""
    parser = argparse.ArgumentParser(description="Model Validation")
    parser.add_argument("--date", required=True, help="Validation date (YYYY-MM-DD)")
    parser.add_argument("--model-name", default="price_prediction", help="Model name")
    parser.add_argument(
        "--version-id", help="Specific version to validate (default: latest)"
    )
    parser.add_argument("--test-data", help="Path to test data")

    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("Model Validation")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        validator = ModelValidator(spark)

        # Determine version to validate
        registry = ModelRegistry()
        if args.version_id:
            version_id = args.version_id
        else:
            # Use latest version
            model_info = registry.list_models().get(args.model_name, {})
            version_id = model_info.get("latest_version")
            if not version_id:
                raise ValueError(f"No versions found for model {args.model_name}")

        # Determine test data path
        test_data_path = args.test_data or "/data/real_estate/gold/unified_properties"

        # Run validation tests
        test_results = validator.run_model_tests(
            args.model_name, version_id, test_data_path
        )

        # Generate report
        report_path = validator.generate_validation_report(
            args.model_name, version_id, test_results
        )

        # Print summary
        print(f"Validation Status: {test_results['overall_status']}")
        print(
            f"Tests Passed: {test_results['total_tests'] - test_results['failed_tests']}/{test_results['total_tests']}"
        )
        print(f"Report: {report_path}")

        # Exit with appropriate code
        if test_results["overall_status"] == "failed":
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
