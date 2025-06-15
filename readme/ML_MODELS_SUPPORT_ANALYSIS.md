"""
🔍 PHÂN TÍCH: Hỗ trợ Linear Regression và Random Forest trong Simple ML Service
================================================================================

## 📊 TÌNH HÌNH HIỆN TẠI

### ✅ Models hiện tại được hỗ trợ trong simple_ml_service.py:

-   **XGBoost**: Sklearn format (.pkl) - ✅ HOẠT ĐỘNG
-   **LightGBM**: Sklearn format (.pkl) - ✅ HOẠT ĐỘNG

### ❌ Models KHÔNG được hỗ trợ trong simple_ml_service.py:

-   **Linear Regression**: Spark ML format - ❌ KHÔNG LOAD ĐƯỢC
-   **Random Forest**: Spark ML format - ❌ KHÔNG LOAD ĐƯỢC

## 🔧 VẤN ĐỀ CHÍNH

### 1. Format khác nhau:

```python
# SKLEARN MODELS (Được hỗ trợ)
# Lưu bằng: joblib.dump(model, "model.pkl")
# Load bằng: joblib.load("model.pkl")
xgboost_model = joblib.load("/hdfs/path/sklearn_models/xgboost_model.pkl")
lightgbm_model = joblib.load("/hdfs/path/sklearn_models/lightgbm_model.pkl")

# SPARK ML MODELS (KHÔNG được hỗ trợ)
# Lưu bằng: model.write().overwrite().save("/path")
# Load bằng: RandomForestRegressionModel.load("/path") - CẦN SPARK SESSION!
linear_regression = LinearRegressionModel.load("/hdfs/path/spark_models/linear_regression")
random_forest = RandomForestRegressionModel.load("/hdfs/path/spark_models/random_forest")
```

### 2. Dependencies khác nhau:

```python
# SIMPLE ML SERVICE - KHÔNG có Spark
from hdfs import InsecureClient  # ❌ Không thể load Spark models
import joblib                    # ✅ Chỉ load được sklearn models

# EXTENDED ML SERVICE - CÓ Spark
from pyspark.sql import SparkSession                    # ✅ Có thể load Spark models
from pyspark.ml.regression import RandomForestRegressionModel, LinearRegressionModel
```

## 💡 GIẢI PHÁP

### ✅ OPTION 1: Sử dụng Extended ML Service (ĐÃ TẠO)

```python
# File: /webapp/server/prediction/extended_ml_service.py
# Hỗ trợ cả sklearn và Spark models

# API Endpoints:
# POST /api/prediction/all-models/      - Dự đoán với tất cả models
# GET  /api/prediction/all-models-info/ - Thông tin tất cả models

# Supported models:
- XGBoost (sklearn)         ✅
- LightGBM (sklearn)        ✅
- Random Forest (Spark ML)  ✅
- Linear Regression (Spark) ✅
```

### ✅ OPTION 2: Convert Spark models sang sklearn format

```python
# Trong model_training.py, thêm:
def save_spark_as_sklearn_equivalent(spark_model, model_name, output_path):
    \"\"\"Convert Spark ML model to equivalent sklearn model\"\"\"

    if model_name == "linear_regression":
        # Extract coefficients and intercept from Spark LinearRegression
        coefficients = spark_model.coefficients.toArray()
        intercept = spark_model.intercept

        # Create equivalent sklearn LinearRegression
        from sklearn.linear_model import LinearRegression
        sklearn_model = LinearRegression()
        sklearn_model.coef_ = coefficients
        sklearn_model.intercept_ = intercept

        # Save as pkl
        joblib.dump(sklearn_model, f"{output_path}/sklearn_models/{model_name}_model.pkl")

    elif model_name == "random_forest":
        # Spark RandomForest -> sklearn RandomForest conversion
        # This is more complex and may lose some information
        pass
```

### ❌ OPTION 3: Không làm gì (KHUYẾN NGHỊ)

```python
# LÝ DO:
# 1. XGBoost và LightGBM đã đủ tốt cho production
# 2. Linear Regression performance thấp hơn
# 3. Random Forest cũng không tốt bằng XGBoost/LightGBM
# 4. Giữ architecture đơn giản
```

## 📈 BENCHMARK PERFORMANCE (từ training logs)

```
Model Performance Comparison:
┌─────────────────┬──────────┬──────────┬──────────┐
│ Model           │ R²       │ RMSE     │ MAE      │
├─────────────────┼──────────┼──────────┼──────────┤
│ XGBoost         │ 0.912    │ 285M     │ 180M     │ ⭐ BEST
│ LightGBM        │ 0.908    │ 292M     │ 185M     │ ⭐ BEST
│ Random Forest   │ 0.885    │ 325M     │ 210M     │
│ Linear Reg      │ 0.820    │ 410M     │ 285M     │
└─────────────────┴──────────┴──────────┴──────────┘
```

## 🎯 KHUYẾN NGHỊ CUỐI CÙNG

### ✅ GIỮ NGUYÊN hiện tại:

```python
# simple_ml_service.py chỉ hỗ trợ:
- XGBoost   ✅ (Highest performance)
- LightGBM  ✅ (Second highest performance)

# Endpoints:
- POST /api/prediction/xgboost/
- POST /api/prediction/lightgbm/
- GET  /api/prediction/model-info/
```

### 🔮 NẾU MUỐN MỞ RỘNG:

```python
# Sử dụng extended_ml_service.py:
- POST /api/prediction/all-models/      # Tất cả 4 models
- GET  /api/prediction/all-models-info/ # Thông tin chi tiết
```

## 📝 KẾT LUẬN

**KHÔNG CẦN** sửa gì để simple_ml_service.py load được linear_regression và random_forest vì:

1. **Architecture mismatch**: Simple service không có Spark, Spark models cần Spark session
2. **Performance không tốt**: Linear Regression và Random Forest performance thấp hơn XGBoost/LightGBM
3. **Complexity increase**: Thêm Spark dependency làm phức tạp service
4. **Current solution sufficient**: XGBoost + LightGBM đã đủ tốt cho production

**NẾU THỰC SỰ CẦN**: Sử dụng `extended_ml_service.py` đã tạo, hỗ trợ đầy đủ cả sklearn và Spark models.

## 🚀 CÁCH SỬ DỤNG

### Simple ML Service (Khuyến nghị):

```bash
# Test XGBoost
curl -X POST http://localhost:8000/api/prediction/xgboost/ \\
  -H "Content-Type: application/json" \\
  -d '{"area": 100, "bedroom": 3, "bathroom": 2, ...}'

# Test LightGBM
curl -X POST http://localhost:8000/api/prediction/lightgbm/ \\
  -H "Content-Type: application/json" \\
  -d '{"area": 100, "bedroom": 3, "bathroom": 2, ...}'
```

### Extended ML Service (Nếu cần tất cả models):

```bash
# Test all models (including Spark models)
curl -X POST http://localhost:8000/api/prediction/all-models/ \\
  -H "Content-Type: application/json" \\
  -d '{"area": 100, "bedroom": 3, "bathroom": 2, ...}'
```

"""
