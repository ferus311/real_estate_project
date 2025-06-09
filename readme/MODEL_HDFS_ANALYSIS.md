# 🔍 Phân Tích Chi Tiết: Models Trên HDFS & Khả Năng Truy Cập

## 📋 TÓM TẮT

**CÂU HỎI CHÍNH:**

1. Code `model_training.py` lưu trữ cái gì lên HDFS chính xác?
2. Sau này có thể truy cập vào models đã lưu trên HDFS và dùng để viết Python scripts dự đoán giá nhà được không?

**CÂU TRẢ LỜI NGẮN GỌN:**
✅ **CÓ!** Hệ thống lưu trữ đầy đủ models và metadata, hoàn toàn có thể viết scripts Python để load và sử dụng.

---

## 🗂️ PHẦN 1: CẤU TRÚC LƯU TRỮ TRÊN HDFS

### 📁 Structure được tạo bởi `save_models()` method:

```
{model_output_path}/{property_type}/{date}/
├── spark_models/                    # Spark ML Models
│   ├── random_forest/              # RandomForest model files
│   ├── gradient_boost/             # GBT model files
│   ├── linear_regression/          # Linear regression files
│   └── decision_tree/              # Decision tree files
│
├── preprocessing_pipeline/          # Data preprocessing pipeline
│   ├── stages/                     # Individual pipeline stages
│   └── metadata/                   # Pipeline metadata
│
├── advanced_models/                 # Sklearn-compatible models
│   ├── xgboost_model.pkl          # XGBoost (joblib format)
│   ├── lightgbm_model.pkl         # LightGBM (joblib format)
│   └── catboost_model.pkl         # CatBoost (joblib format)
│
└── model_registry.json             # Model metadata & metrics
```

### 📊 Chi tiết nội dung từng thành phần:

#### 1. **Spark Models** (spark_models/)

```python
# Mỗi model được lưu với Spark's native format
for name, model in models.items():
    if model["model_type"] == "spark":
        spark_model_path = f"{model_dir}/spark_models/{name}"
        model["model"].write().overwrite().save(spark_model_path)
```

#### 2. **Preprocessing Pipeline** (preprocessing_pipeline/)

```python
# Pipeline xử lý dữ liệu đầu vào
preprocessing_path = f"{model_dir}/preprocessing_pipeline"
preprocessing_model.write().overwrite().save(preprocessing_path)
```

#### 3. **Advanced Models** (advanced_models/)

```python
# Sklearn-compatible models với joblib compression
for name, model in models.items():
    if model["model_type"] == "sklearn":
        model_file = f"{model_dir}/advanced_models/{name}_model.pkl"
        joblib.dump(model["model"], model_file, compress=3)
```

#### 4. **Model Registry** (model_registry.json)

```json
{
    "model_version": "v20250605_143022",
    "training_date": "2025-06-05",
    "timestamp": "20250605_143022",
    "best_model": {
        "name": "xgboost",
        "rmse": 25000000.0,
        "mae": 18000000.0,
        "r2": 0.85,
        "model_type": "sklearn"
    },
    "all_models": {
        "random_forest": { "rmse": 28000000.0, "mae": 20000000.0, "r2": 0.82 },
        "xgboost": { "rmse": 25000000.0, "mae": 18000000.0, "r2": 0.85 },
        "lightgbm": { "rmse": 26000000.0, "mae": 19000000.0, "r2": 0.84 }
    },
    "ensemble": {
        "models": ["xgboost", "lightgbm", "random_forest"],
        "weights": { "xgboost": 0.5, "lightgbm": 0.3, "random_forest": 0.2 }
    },
    "deployment_ready": true,
    "production_metrics": {
        "target_rmse_threshold": 27500000.0,
        "minimum_r2": 0.7,
        "passed_validation": true
    }
}
```

---

## 🔧 PHẦN 2: CÁCH TRUY CẬP & SỬ DỤNG MODELS

### 🚀 Tạo Model Loader Utility

Dựa trên hệ thống hiện tại, bạn có thể tạo utility để load models:

```python
# utils/model_loader.py
import os
import json
import joblib
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

class ModelLoader:
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder.appName("ModelLoader").getOrCreate()

    def load_model_registry(self, model_path):
        """Load model registry để biết model nào là best"""
        registry_path = f"{model_path}/model_registry.json"

        # Read JSON from Spark DataFrame format
        registry_df = self.spark.read.json(registry_path)
        registry_json = registry_df.collect()[0]['registry']
        return json.loads(registry_json)

    def load_best_model(self, model_path):
        """Load best model dựa trên registry"""
        registry = self.load_model_registry(model_path)
        best_model_info = registry['best_model']

        if best_model_info['model_type'] == 'spark':
            # Load Spark model
            model_file = f"{model_path}/spark_models/{best_model_info['name']}"
            return PipelineModel.load(model_file)

        elif best_model_info['model_type'] == 'sklearn':
            # Load sklearn model
            model_file = f"{model_path}/advanced_models/{best_model_info['name']}_model.pkl"
            return joblib.load(model_file)

    def load_preprocessing_pipeline(self, model_path):
        """Load preprocessing pipeline"""
        pipeline_path = f"{model_path}/preprocessing_pipeline"
        return PipelineModel.load(pipeline_path)
```

### 🎯 Script Dự Đoán Giá Nhà

```python
# scripts/house_price_predictor.py
from utils.model_loader import ModelLoader
from pyspark.sql import SparkSession
import pandas as pd

class HousePricePredictor:
    def __init__(self, model_date="2025-06-05", property_type="house"):
        self.spark = SparkSession.builder.appName("HousePricePredictor").getOrCreate()
        self.loader = ModelLoader(self.spark)

        # Đường dẫn model dựa trên date
        self.model_path = f"/data/realestate/processed/ml/models/{property_type}/{model_date}"

        # Load models
        self.registry = self.loader.load_model_registry(self.model_path)
        self.preprocessing_pipeline = self.loader.load_preprocessing_pipeline(self.model_path)
        self.best_model = self.loader.load_best_model(self.model_path)

    def predict_single_house(self, house_data):
        """Dự đoán giá cho 1 căn nhà"""
        # Convert to Spark DataFrame
        df = self.spark.createDataFrame([house_data])

        # Apply preprocessing pipeline
        processed_df = self.preprocessing_pipeline.transform(df)

        # Predict với best model
        if self.registry['best_model']['model_type'] == 'spark':
            predictions = self.best_model.transform(processed_df)
            return predictions.select("prediction").collect()[0]['prediction']

        else:  # sklearn model
            # Extract features vector
            features = processed_df.select("features").collect()[0]['features']
            # Convert Spark vector to numpy array
            feature_array = features.toArray().reshape(1, -1)
            return self.best_model.predict(feature_array)[0]

    def predict_batch(self, houses_data):
        """Dự đoán giá cho nhiều căn nhà"""
        df = self.spark.createDataFrame(houses_data)
        processed_df = self.preprocessing_pipeline.transform(df)

        if self.registry['best_model']['model_type'] == 'spark':
            predictions = self.best_model.transform(processed_df)
            return predictions.select("id", "prediction").toPandas()
        else:
            # Handle sklearn batch prediction
            features_list = processed_df.select("features").collect()
            predictions = []
            for row in features_list:
                feature_array = row['features'].toArray().reshape(1, -1)
                pred = self.best_model.predict(feature_array)[0]
                predictions.append(pred)
            return predictions

# Sử dụng:
predictor = HousePricePredictor(model_date="2025-06-05")

# Dự đoán 1 căn nhà
house = {
    "area_cleaned": 120.0,
    "bedrooms_cleaned": 3,
    "bathrooms_cleaned": 2,
    "city": "Ho Chi Minh City",
    "district": "District 1"
}

predicted_price = predictor.predict_single_house(house)
print(f"Predicted price: {predicted_price:,.0f} VND")
```

---

## 📈 PHẦN 3: KIỂM TRA THỰC TẾ

### 🔍 Kiểm tra models hiện có:

```bash
# Kiểm tra structure trên HDFS
hdfs dfs -ls /data/realestate/processed/ml/models/house/

# Xem chi tiết 1 model folder
hdfs dfs -ls /data/realestate/processed/ml/models/house/2025-06-05/
```

### 🧪 Test model loading:

```python
# test_model_access.py
from utils.model_loader import ModelLoader

# Test load registry
loader = ModelLoader()
registry = loader.load_model_registry("/data/realestate/processed/ml/models/house/2025-06-05")
print("Best model:", registry['best_model']['name'])
print("R²:", registry['best_model']['r2'])

# Test load actual model
model = loader.load_best_model("/data/realestate/processed/ml/models/house/2025-06-05")
print("Model loaded successfully:", type(model))
```

---

## ✅ PHẦN 4: KẾT LUẬN

### 🎯 Trả lời câu hỏi gốc:

1. **Code lưu gì lên HDFS?**

    - ✅ Spark ML models (native format)
    - ✅ Advanced models: XGBoost, LightGBM, CatBoost (joblib .pkl)
    - ✅ Preprocessing pipeline (Spark format)
    - ✅ Model registry với metrics và metadata (JSON)

2. **Có thể viết Python scripts dự đoán không?**
    - ✅ **CÓ!** Hoàn toàn có thể
    - ✅ Support cả Spark và sklearn models
    - ✅ Có đầy đủ preprocessing pipeline
    - ✅ Có metadata để identify best model

### 🚀 Kiến trúc đã confirm:

```
Training Pipeline → Save Best 4 Models to HDFS → Prediction Server using saved models + PostgreSQL
```

**Status:** ✅ **FEASIBLE & READY TO IMPLEMENT**

### 📝 Next Steps:

1. Tạo `ModelLoader` utility class
2. Implement prediction scripts
3. Set up Django API endpoints để serve predictions
4. Connect PostgreSQL cho data storage và logging
5. Deploy prediction server

Hệ thống đã sẵn sàng để implement prediction service từ trained models trên HDFS!
