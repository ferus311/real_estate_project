"""
ML Model Inference cho Real Estate Price Prediction
Đọc model đã train → Load Gold data → Predict → Save results
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.ml import PipelineModel
import logging

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from spark.common.config.spark_config import create_spark_session
from spark.common.utils.hdfs_utils import check_hdfs_path_exists
from spark.common.utils.date_utils import get_date_format
from data_preprocessing import RealEstateDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealEstateMLPredictor:
    """ML Inference for Real Estate Price Prediction"""

    def __init__(self, model_base_path="/data/realestate/ml/models"):
        self.spark = create_spark_session("RealEstate_ML_Inference")
        self.model_base_path = model_base_path
        self.data_processor = RealEstateDataProcessor(self.spark)

    def load_model(self, model_date, model_name="random_forest"):
        """Load trained model from HDFS"""
        model_path = f"{self.model_base_path}/{model_date}/{model_name}_spark_model"
        preprocessing_path = (
            f"{self.model_base_path}/{model_date}/{model_name}_preprocessing"
        )

        if not check_hdfs_path_exists(self.spark, model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")

        if not check_hdfs_path_exists(self.spark, preprocessing_path):
            raise FileNotFoundError(
                f"Preprocessing pipeline not found: {preprocessing_path}"
            )

        # Load Spark ML models
        from pyspark.ml.regression import (
            RandomForestRegressionModel,
            GBTRegressionModel,
            LinearRegressionModel,
        )

        try:
            if model_name == "random_forest":
                model = RandomForestRegressionModel.load(model_path)
            elif model_name == "gradient_boost":
                model = GBTRegressionModel.load(model_path)
            elif model_name == "linear_regression":
                model = LinearRegressionModel.load(model_path)
            else:
                raise ValueError(f"Unknown model type: {model_name}")

            preprocessing_model = PipelineModel.load(preprocessing_path)

            logger.info(f"Successfully loaded model: {model_name} from {model_date}")
            return model, preprocessing_model

        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    def read_data_for_prediction(self, date, property_type="house"):
        """Read data for prediction (can be Gold data or new data)"""
        # Same logic as training pipeline to read Gold data
        gold_path = f"/data/realestate/processed/gold/unified/{property_type}"
        date_formatted = date.replace("-", "")

        possible_paths = [
            f"{gold_path}/{date}/unified_{property_type}_{date_formatted}.parquet",
            f"{gold_path}/{date.replace('-', '/')}/unified_{property_type}_{date_formatted}.parquet",
        ]

        for path in possible_paths:
            if check_hdfs_path_exists(self.spark, path):
                logger.info(f"Reading data for prediction from: {path}")
                df = self.spark.read.parquet(path)
                logger.info(f"Loaded {df.count()} records for prediction")
                return df

        raise FileNotFoundError(f"No data found for prediction on date {date}")

    def prepare_prediction_data(self, df):
        """Prepare data for prediction using same preprocessing as training"""
        logger.info("Preparing prediction data with comprehensive preprocessing...")

        # Use the same comprehensive preprocessing as training
        df_processed, numeric_features, categorical_features, quality_stats = (
            self.data_processor.run_full_preprocessing(df)
        )

        logger.info(f"Prediction data preprocessing completed")
        logger.info(f"Records after preprocessing: {df_processed.count()}")

        return df_processed

    def predict(self, model, preprocessing_model, df):
        """Run prediction on prepared data"""
        logger.info("Running predictions...")

        # Apply preprocessing
        df_preprocessed = preprocessing_model.transform(df)

        # Run prediction
        predictions = model.transform(df_preprocessed)

        # Select useful columns for output
        result_df = predictions.select(
            col("id"),
            col("title"),
            col("area"),
            col("province"),
            col("price").alias("actual_price"),
            col("prediction").alias("predicted_price"),
            (col("prediction") / col("area")).alias("predicted_price_per_sqm"),
            current_timestamp().alias("prediction_timestamp"),
        )

        logger.info(f"Generated {result_df.count()} predictions")
        return result_df

    def save_predictions(self, predictions_df, output_date):
        """Save predictions to HDFS"""
        output_path = f"/data/realestate/ml/predictions/{output_date}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_path = f"{output_path}/predictions_{timestamp}.parquet"

        logger.info(f"Saving predictions to: {final_path}")

        predictions_df.write.mode("overwrite").parquet(final_path)

        logger.info(f"Predictions saved successfully")
        return final_path

    def run_prediction_pipeline(
        self,
        model_date,
        data_date=None,
        model_name="random_forest",
        property_type="house",
    ):
        """Run complete prediction pipeline"""
        logger.info("=== Starting Real Estate ML Prediction Pipeline ===")

        if data_date is None:
            data_date = get_date_format()

        try:
            # 1. Load trained model
            model, preprocessing_model = self.load_model(model_date, model_name)

            # 2. Read data for prediction
            df = self.read_data_for_prediction(data_date, property_type)

            # 3. Prepare data
            df_prepared = self.prepare_prediction_data(df)

            # 4. Run predictions
            predictions = self.predict(model, preprocessing_model, df_prepared)

            # 5. Save results
            output_path = self.save_predictions(predictions, data_date)

            # 6. Show sample results
            logger.info("Sample predictions:")
            predictions.select("title", "actual_price", "predicted_price").show(
                5, truncate=False
            )

            logger.info("=== Prediction Pipeline Completed Successfully ===")

            return {
                "predictions_path": output_path,
                "total_predictions": predictions.count(),
                "model_used": model_name,
                "model_date": model_date,
            }

        except Exception as e:
            logger.error(f"Prediction pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description="Real Estate ML Prediction Pipeline")
    parser.add_argument(
        "--model-date", required=True, help="Model training date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--data-date", help="Data date for prediction (YYYY-MM-DD)", default=None
    )
    parser.add_argument("--model-name", help="Model name", default="random_forest")
    parser.add_argument("--property-type", help="Property type", default="house")

    args = parser.parse_args()

    # Run prediction
    predictor = RealEstateMLPredictor()
    result = predictor.run_prediction_pipeline(
        args.model_date, args.data_date, args.model_name, args.property_type
    )

    print("Prediction Results:")
    print(f"Total Predictions: {result['total_predictions']}")
    print(f"Model Used: {result['model_used']} (trained on {result['model_date']})")
    print(f"Output Path: {result['predictions_path']}")


if __name__ == "__main__":
    main()
