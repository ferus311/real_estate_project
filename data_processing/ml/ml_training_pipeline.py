"""
ML Training Pipeline cho Real Estate Project
Đọc ML features từ feature store → Model Training → Save Model
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, log, sqrt, isnan, isnull
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import joblib
import logging

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from common.config.spark_config import create_spark_session
from common.utils.hdfs_utils import check_hdfs_path_exists
from common.utils.date_utils import get_date_format

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealEstateMLTrainer:
    """Simple ML Training Pipeline for Real Estate Price Prediction"""

    def __init__(self, model_output_path="/data/realestate/ml/models"):
        self.spark = create_spark_session("RealEstate_ML_Training")
        self.model_output_path = model_output_path
        self.model_registry = {}

    def read_gold_data(self, date=None, property_type="house"):
        """Read Gold data from HDFS"""
        if date is None:
            date = get_date_format()

        # Gold data path pattern
        gold_path = f"/data/realestate/processed/gold/unified/{property_type}"

        # Try different date formats
        date_formatted = date.replace("-", "")
        possible_paths = [
            f"{gold_path}/{date}/unified_{property_type}_{date_formatted}.parquet",
            f"{gold_path}/{date.replace('-', '/')}/unified_{property_type}_{date_formatted}.parquet",
        ]

        for path in possible_paths:
            if check_hdfs_path_exists(self.spark, path):
                logger.info(f"Reading Gold data from: {path}")
                df = self.spark.read.parquet(path)
                logger.info(f"Loaded {df.count()} records from Gold data")
                return df

        # If no exact date found, try to find recent data
        logger.warning(f"No data found for date {date}, looking for recent data...")
        return self._find_recent_data(gold_path, property_type)

    def _find_recent_data(self, base_path, property_type, days_back=7):
        """Find most recent available data within last N days"""
        current_date = datetime.now()

        for i in range(days_back):
            check_date = current_date - timedelta(days=i)
            date_str = check_date.strftime("%Y-%m-%d")
            date_formatted = check_date.strftime("%Y%m%d")

            possible_paths = [
                f"{base_path}/{date_str}/unified_{property_type}_{date_formatted}.parquet",
                f"{base_path}/{check_date.strftime('%Y/%m/%d')}/unified_{property_type}_{date_formatted}.parquet",
            ]

            for path in possible_paths:
                if check_hdfs_path_exists(self.spark, path):
                    logger.info(f"Found recent data: {path}")
                    df = self.spark.read.parquet(path)
                    return df

        raise FileNotFoundError(f"No Gold data found in last {days_back} days")

    def read_ml_features(self, date, property_type="house"):
        """Read ML features from feature store (created by Spark job)"""
        feature_path = (
            f"/data/realestate/ml/features/{property_type}/{date}/features.parquet"
        )
        metadata_path = (
            f"/data/realestate/ml/features/{property_type}/{date}/feature_metadata.json"
        )

        if not check_hdfs_path_exists(self.spark, feature_path):
            logger.error(f"ML features not found: {feature_path}")
            logger.info("Please run feature engineering first:")
            logger.info(
                f"python /data_processing/spark/jobs/enrichment/ml_feature_engineering.py --date {date}"
            )
            raise FileNotFoundError(f"ML features not found: {feature_path}")

        logger.info(f"Reading ML features from: {feature_path}")
        df = self.spark.read.parquet(feature_path)

        # Read feature metadata
        try:
            metadata_df = self.spark.read.json(metadata_path)
            metadata_json = metadata_df.collect()[0]["metadata"]
            import json

            metadata = json.loads(metadata_json)

            logger.info(f"Feature metadata loaded:")
            logger.info(f"- Numeric features: {len(metadata['numeric_features'])}")
            logger.info(
                f"- Categorical features: {len(metadata['categorical_features'])}"
            )
            logger.info(f"- Binary features: {len(metadata['binary_features'])}")
            logger.info(f"- Total records: {metadata['total_records']:,}")

            return df, metadata

        except Exception as e:
            logger.warning(f"Could not load feature metadata: {e}")
            return df, None

    def prepare_ml_data(self, df, metadata):
        """Prepare data for ML training using features from feature store"""
        logger.info("Preparing ML features for training...")

        if metadata is None:
            logger.warning("No metadata available, using default feature selection")
            # Fallback feature selection
            numeric_features = [
                "price",
                "area",
                "log_price",
                "log_area",
                "sqrt_area",
                "price_per_sqm",
            ]
            categorical_features = ["province_clean", "region"]
            binary_features = ["is_major_city"]
        else:
            numeric_features = metadata["numeric_features"]
            categorical_features = metadata["categorical_features"]
            binary_features = metadata["binary_features"]

        # Create indexers for categorical features
        indexers = []
        indexed_categorical = []

        for cat_feature in categorical_features:
            if cat_feature in df.columns:
                indexer_name = f"{cat_feature}_indexed"
                indexer = StringIndexer(
                    inputCol=cat_feature, outputCol=indexer_name, handleInvalid="keep"
                )
                indexers.append(indexer)
                indexed_categorical.append(indexer_name)

        # Select available features only
        available_numeric = [f for f in numeric_features if f in df.columns]
        available_binary = [f for f in binary_features if f in df.columns]
        all_features = available_numeric + indexed_categorical + available_binary

        # Assemble all features
        assembler = VectorAssembler(
            inputCols=all_features, outputCol="features_raw", handleInvalid="skip"
        )

        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw", outputCol="features", withStd=True, withMean=True
        )

        # Create preprocessing pipeline
        preprocessing_stages = indexers + [assembler, scaler]
        preprocessing_pipeline = Pipeline(stages=preprocessing_stages)

        # Fit and transform
        preprocessing_model = preprocessing_pipeline.fit(df)
        df_prepared = preprocessing_model.transform(df)

        # Select final columns for ML
        final_df = df_prepared.select("features", "price", "log_price")

        logger.info(f"ML data prepared. Final records: {final_df.count()}")
        logger.info(f"Feature vector size: {len(all_features)}")

        return final_df, preprocessing_model

    def train_models(self, df):
        """Train multiple regression models"""
        logger.info("Starting model training...")

        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        logger.info(f"Train set: {train_df.count()}, Test set: {test_df.count()}")

        models = {}
        evaluator = RegressionEvaluator(
            labelCol="price", predictionCol="prediction", metricName="rmse"
        )

        # 1. Random Forest
        rf = RandomForestRegressor(
            featuresCol="features", labelCol="price", numTrees=50, maxDepth=10, seed=42
        )
        rf_model = rf.fit(train_df)
        rf_predictions = rf_model.transform(test_df)
        rf_rmse = evaluator.evaluate(rf_predictions)
        models["random_forest"] = {
            "model": rf_model,
            "rmse": rf_rmse,
            "predictions": rf_predictions,
        }
        logger.info(f"Random Forest RMSE: {rf_rmse:,.0f}")

        # 2. Gradient Boosted Trees
        gbt = GBTRegressor(
            featuresCol="features", labelCol="price", maxIter=50, maxDepth=8, seed=42
        )
        gbt_model = gbt.fit(train_df)
        gbt_predictions = gbt_model.transform(test_df)
        gbt_rmse = evaluator.evaluate(gbt_predictions)
        models["gradient_boost"] = {
            "model": gbt_model,
            "rmse": gbt_rmse,
            "predictions": gbt_predictions,
        }
        logger.info(f"Gradient Boost RMSE: {gbt_rmse:,.0f}")

        # 3. Linear Regression (baseline)
        lr = LinearRegression(featuresCol="features", labelCol="price", regParam=0.1)
        lr_model = lr.fit(train_df)
        lr_predictions = lr_model.transform(test_df)
        lr_rmse = evaluator.evaluate(lr_predictions)
        models["linear_regression"] = {
            "model": lr_model,
            "rmse": lr_rmse,
            "predictions": lr_predictions,
        }
        logger.info(f"Linear Regression RMSE: {lr_rmse:,.0f}")

        # Find best model
        best_model_name = min(models.keys(), key=lambda k: models[k]["rmse"])
        best_model = models[best_model_name]

        logger.info(
            f"Best model: {best_model_name} with RMSE: {best_model['rmse']:,.0f}"
        )

        return models, best_model_name

    def save_model(self, model_info, model_name, preprocessing_model, date):
        """Save trained model to HDFS"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_path = f"{self.model_output_path}/{date}"

        # Create model metadata
        metadata = {
            "model_name": model_name,
            "model_type": type(model_info["model"]).__name__,
            "rmse": model_info["rmse"],
            "training_date": date,
            "timestamp": timestamp,
            "model_path": model_path,
        }

        try:
            # Save Spark ML model (this saves to HDFS automatically)
            spark_model_path = f"{model_path}/{model_name}_spark_model"
            model_info["model"].write().overwrite().save(spark_model_path)

            # Save preprocessing pipeline
            preprocessing_path = f"{model_path}/{model_name}_preprocessing"
            preprocessing_model.write().overwrite().save(preprocessing_path)

            logger.info(f"Model saved to HDFS: {model_path}")

            # Store in registry
            self.model_registry[model_name] = metadata

            return model_path

        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise

    def run_training_pipeline(self, date=None, property_type="house"):
        """Run complete ML training pipeline"""
        logger.info("=== Starting Real Estate ML Training Pipeline ===")

        try:
            logger.info("=== Starting ML Training Pipeline ===")
            logger.info(f"Date: {date or get_date_format()}")
            logger.info(f"Property Type: {property_type}")

            # 1. Read ML features from feature store (created by Spark job)
            df, metadata = self.read_ml_features(
                date or get_date_format(), property_type
            )

            # 2. Prepare ML data (features already engineered)
            df_ml, preprocessing_model = self.prepare_ml_data(df, metadata)

            # 3. Train models
            models, best_model_name = self.train_models(df_ml)

            # 4. Save best model
            model_path = self.save_model(
                models[best_model_name],
                best_model_name,
                preprocessing_model,
                date or get_date_format(),
            )

            logger.info("=== Training Pipeline Completed Successfully ===")
            logger.info(f"Best model: {best_model_name}")
            logger.info(f"Model saved to: {model_path}")

            return {
                "best_model": best_model_name,
                "model_path": model_path,
                "rmse": models[best_model_name]["rmse"],
                "all_models": {k: v["rmse"] for k, v in models.items()},
            }

        except Exception as e:
            logger.error(f"Training pipeline failed: {e}")
            raise
        finally:
            # Clean up
            self.spark.stop()


def run_ml_training(spark=None, input_date=None, property_type="house"):
    """Run the ML training pipeline with specified date and property type"""
    trainer = RealEstateMLTrainer()
    # Use provided spark session or create new one
    if spark:
        trainer.spark = spark
    return trainer.run_training_pipeline(input_date, property_type)


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description="Real Estate ML Training Pipeline")
    parser.add_argument("--date", help="Training date (YYYY-MM-DD)", default=None)
    parser.add_argument("--property-type", help="Property type", default="house")

    args = parser.parse_args()

    # Run training
    trainer = RealEstateMLTrainer()
    result = trainer.run_training_pipeline(args.date, args.property_type)

    print("Training Results:")
    print(f"Best Model: {result['best_model']}")
    print(f"RMSE: {result['rmse']:,.0f}")
    print(f"Model Path: {result['model_path']}")


if __name__ == "__main__":
    main()
