#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
🚀 ML Performance Optimization Utilities
========================================

This module provides utilities to optimize Spark ML performance and
eliminate "Broadcasting large task binary" warnings.

Key optimization strategies:
1. Reduce closure capture overhead
2. Use broadcast variables efficiently
3. Minimize object serialization
4. Optimize data handling patterns
5. Memory management best practices

Author: ML Team
Date: June 2025
"""

import sys
import os
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast
from pyspark.ml.feature import VectorAssembler
import warnings

warnings.filterwarnings("ignore")


class SparkMLOptimizer:
    """
    ⚡ Spark ML Performance Optimizer

    Provides methods to optimize Spark ML operations and prevent
    large task binary broadcasts.
    """

    @staticmethod
    def optimize_dataframe_for_ml(df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Optimize DataFrame for ML operations

        Args:
            df: Input DataFrame
            spark: SparkSession

        Returns:
            Optimized DataFrame
        """
        # Checkpoint to break lineage and reduce serialization overhead
        df_optimized = df.repartition(
            spark.sparkContext.defaultParallelism
        ).checkpoint()

        # Force materialization
        df_optimized.count()

        return df_optimized

    @staticmethod
    def create_broadcast_config(config_dict: Dict[str, Any], spark: SparkSession):
        """
        Create broadcast variable for configuration to avoid repeated serialization

        Args:
            config_dict: Configuration dictionary
            spark: SparkSession

        Returns:
            Broadcast variable
        """
        return spark.sparkContext.broadcast(config_dict)

    @staticmethod
    def efficient_feature_extraction(
        df: DataFrame, feature_columns: List[str]
    ) -> DataFrame:
        """
        Efficiently extract features with minimal serialization

        Args:
            df: Input DataFrame
            feature_columns: List of feature column names

        Returns:
            DataFrame with features column
        """
        # Use VectorAssembler with minimal configuration
        assembler = VectorAssembler(
            inputCols=feature_columns, outputCol="features", handleInvalid="skip"
        )

        return assembler.transform(df)

    @staticmethod
    def optimize_pandas_conversion(
        df: DataFrame, max_records: int = 500000
    ) -> pd.DataFrame:
        """
        Optimize Spark to Pandas conversion with sampling

        Args:
            df: Spark DataFrame
            max_records: Maximum records to convert

        Returns:
            Pandas DataFrame
        """
        total_records = df.count()

        if total_records > max_records:
            sample_ratio = max_records / total_records
            df_sampled = df.sample(
                withReplacement=False, fraction=sample_ratio, seed=42
            )
            print(
                f"⚡ Sampling {sample_ratio:.3f} of data ({max_records:,} / {total_records:,} records)"
            )
        else:
            df_sampled = df

        return df_sampled.toPandas()

    @staticmethod
    def clean_spark_cache(spark: SparkSession):
        """
        Clean Spark cache to free memory

        Args:
            spark: SparkSession
        """
        spark.catalog.clearCache()

    @staticmethod
    def get_optimized_spark_config() -> Dict[str, str]:
        """
        Get optimized Spark configuration for ML workloads

        Returns:
            Configuration dictionary
        """
        return {
            # Adaptive Query Execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # Serialization
            # Memory Management
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            # Broadcast Settings
            "spark.sql.broadcastTimeout": "300",
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "50MB",
            # Performance Tuning
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            # Checkpointing
            "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoint",
        }


class ModelSerializationOptimizer:
    """
    🔧 Model Serialization Optimizer

    Utilities to optimize model serialization and reduce memory overhead.
    """

    @staticmethod
    def create_lightweight_model_wrapper(model, model_type: str):
        """
        Create lightweight wrapper for models to reduce serialization overhead

        Args:
            model: ML model
            model_type: Type of model ('spark' or 'sklearn')

        Returns:
            Lightweight model wrapper
        """
        return {
            "model_id": id(model),
            "model_type": model_type,
            "model_class": type(model).__name__,
            # Don't store the actual model to avoid serialization
        }

    @staticmethod
    def optimize_model_storage(models: Dict[str, Any], output_path: str):
        """
        Optimize model storage with compression

        Args:
            models: Dictionary of models
            output_path: Output path for models
        """
        import joblib

        for name, model in models.items():
            if model.get("model_type") == "sklearn":
                # Use joblib with compression for sklearn models
                model_path = f"{output_path}/{name}_optimized.pkl"
                joblib.dump(
                    model["model"],
                    model_path,
                    compress=3,  # High compression
                    protocol=4,  # Latest pickle protocol
                )

    @staticmethod
    def create_model_registry_optimized(models: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create optimized model registry with minimal serialization

        Args:
            models: Dictionary of models

        Returns:
            Optimized model registry
        """
        registry = {
            "models": {},
            "best_model": None,
            "metadata": {
                "total_models": len(models),
                "optimization_applied": True,
                "serialization_minimized": True,
            },
        }

        best_r2 = -1
        for name, model in models.items():
            # Store only essential metrics, not the model itself
            registry["models"][name] = {
                "rmse": float(model["rmse"]),
                "mae": float(model["mae"]),
                "r2": float(model["r2"]),
                "model_type": model["model_type"],
            }

            if model["r2"] > best_r2:
                best_r2 = model["r2"]
                registry["best_model"] = name

        return registry


class DataOptimizer:
    """
    📊 Data Processing Optimizer

    Utilities to optimize data processing and reduce Spark overhead.
    """

    @staticmethod
    def optimize_feature_pipeline(feature_cols: List[str], spark: SparkSession):
        """
        Create optimized feature processing pipeline

        Args:
            feature_cols: List of feature columns
            spark: SparkSession

        Returns:
            Optimized pipeline function
        """
        # Broadcast feature columns to avoid repeated serialization
        broadcast_cols = spark.sparkContext.broadcast(feature_cols)

        def process_features(df: DataFrame) -> DataFrame:
            cols = broadcast_cols.value
            return df.select(*cols)

        return process_features

    @staticmethod
    def create_efficient_train_test_split(
        df: DataFrame, test_ratio: float = 0.2, seed: int = 42
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Create efficient train/test split with caching

        Args:
            df: Input DataFrame
            test_ratio: Test set ratio
            seed: Random seed

        Returns:
            Tuple of (train_df, test_df)
        """
        train_df, test_df = df.randomSplit([1 - test_ratio, test_ratio], seed=seed)

        # Cache both DataFrames
        train_df.cache()
        test_df.cache()

        # Force materialization
        train_count = train_df.count()
        test_count = test_df.count()

        print(f"⚡ Train: {train_count:,}, Test: {test_count:,}")

        return train_df, test_df

    @staticmethod
    def optimize_categorical_encoding(df: DataFrame, categorical_cols: List[str]):
        """
        Optimize categorical encoding with minimal serialization

        Args:
            df: Input DataFrame
            categorical_cols: Categorical columns

        Returns:
            Optimized encoding function
        """
        from pyspark.ml.feature import StringIndexer, OneHotEncoder
        from pyspark.ml import Pipeline

        stages = []

        for col_name in categorical_cols:
            indexer = StringIndexer(
                inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep"
            )
            stages.append(indexer)

        pipeline = Pipeline(stages=stages)
        return pipeline


def apply_performance_optimizations(spark: SparkSession) -> SparkSession:
    """
    Apply performance optimizations to Spark session

    Args:
        spark: SparkSession

    Returns:
        Optimized SparkSession
    """
    optimizer = SparkMLOptimizer()
    config = optimizer.get_optimized_spark_config()

    # Apply configurations
    for key, value in config.items():
        spark.conf.set(key, value)

    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    print("⚡ Performance optimizations applied to Spark session")
    return spark


def monitor_broadcast_warnings(spark: SparkSession):
    """
    Monitor and log broadcast warnings

    Args:
        spark: SparkSession
    """
    # Get current log level
    log_level = spark.sparkContext.getConf().get("spark.log.level", "INFO")

    print(f"📊 Current Spark log level: {log_level}")
    print("⚡ To monitor broadcast warnings, check Spark UI at http://localhost:4040")
    print("🔍 Look for 'Broadcasting large task binary' warnings in the stages")


def cleanup_spark_resources(spark: SparkSession, dataframes: List[DataFrame] = None):
    """
    Clean up Spark resources to prevent memory issues

    Args:
        spark: SparkSession
        dataframes: List of DataFrames to unpersist
    """
    if dataframes:
        for df in dataframes:
            if hasattr(df, "unpersist"):
                df.unpersist()

    # Clear catalog cache
    spark.catalog.clearCache()

    # Force garbage collection
    import gc

    gc.collect()

    print("🧹 Spark resources cleaned up")


# Performance monitoring utilities
class PerformanceMonitor:
    """
    📈 Performance Monitor for Spark ML Operations
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.metrics = {}

    def start_monitoring(self, operation_name: str):
        """Start monitoring an operation"""
        import time

        self.start_time = time.time()
        self.operation_name = operation_name
        print(f"⏱️ Starting monitoring: {operation_name}")

    def end_monitoring(self):
        """End monitoring and report metrics"""
        if self.start_time:
            import time

            duration = time.time() - self.start_time

            # Get Spark metrics
            sc = self.spark.sparkContext
            status = sc.statusTracker()

            self.metrics = {
                "duration_seconds": duration,
                "active_jobs": len(status.getActiveJobIds()),
                "active_stages": len(status.getActiveStageIds()),
            }

            print(f"⏱️ {self.operation_name} completed in {duration:.2f}s")
            print(f"📊 Active jobs: {self.metrics['active_jobs']}")
            print(f"📊 Active stages: {self.metrics['active_stages']}")

            return self.metrics

        return None
