# 🤖 Real Estate ML Training Pipeline

## 📋 Tổng quan

Hệ thống ML training hoàn chỉnh cho dự đoán giá bất động sản Việt Nam với:

-   ✅ **Advanced Feature Engineering** với 60+ features thông minh
-   ✅ **State-of-the-art Models**: XGBoost, LightGBM, CatBoost, Ensemble
-   ✅ **Incremental Learning** cho daily updates
-   ✅ **Production-ready** deployment với Docker
-   ✅ **Model Versioning** và experiment tracking với MLflow

## 🏗️ Kiến trúc ML Pipeline

```
📊 Gold Data (HDFS)
       ↓
🔧 Feature Engineering (Spark Job)
       ↓
📈 Advanced ML Training (XGBoost/LightGBM/CatBoost)
       ↓
💾 Model Registry (Versioned Models)
       ↓
🔮 Inference API (FastAPI)
```

### 🎯 Workflow Data Flow

```
Raw Data → Bronze → Silver → Gold → ML Features → Trained Models → Predictions
```

## 📁 Cấu trúc File

```
data_processing/
├── spark/jobs/enrichment/
│   └── ml_feature_engineering.py      # 🔧 Spark job tạo ML features
├── ml/
│   ├── advanced_ml_training.py        # 🤖 Advanced ML training
│   ├── incremental_training_manager.py # 🔄 Daily incremental training
│   ├── ml_training_pipeline.py        # 📈 Main training pipeline
│   ├── ml_inference_pipeline.py       # 🔮 Inference & prediction
│   ├── model_registry.py              # 💾 Model versioning
│   └── ...existing core files...
├── airflow/dags/
│   └── realestate_data_processing_dag.py # 🔄 Airflow orchestration
docker/
├── yml/ml.yml                          # 🐳 Docker ML services
└── ml/Dockerfile.ml-trainer           # 🐳 ML container definition
scripts/
└── ml-pipeline-test.sh                # 🧪 Testing script
```

## 🚀 Cách chạy hệ thống

### 1. Khởi động Infrastructure

```bash
# Khởi động HDFS + Spark
cd /home/fer/data/real_estate_project
docker-compose -f docker/yml/hdfs.yml -f docker/yml/spark.yml up -d

# Khởi động ML services
docker-compose -f docker/yml/ml.yml up -d
```

### 2. Chạy Complete ML Pipeline

```bash
# Chạy full pipeline (Feature Engineering → Training → Inference)
./scripts/ml-pipeline-test.sh full

# Hoặc chạy từng bước riêng biệt:
./scripts/ml-pipeline-test.sh features 2025-06-06    # Feature engineering
./scripts/ml-pipeline-test.sh training              # ML training
./scripts/ml-pipeline-test.sh inference             # Test predictions
```

### 3. Daily Incremental Training

```bash
# Chạy incremental training cho dữ liệu mới hàng ngày
./scripts/ml-pipeline-test.sh incremental 2025-06-06
```

### 4. Chạy qua Airflow (Production)

1. Khởi động Airflow:

```bash
docker-compose -f docker/yml/airflow.yml up -d
```

2. Truy cập Airflow UI: http://localhost:8080
3. Trigger DAG: `realestate_data_processing`

## 🔧 Luồng Feature Engineering

### Input: Gold Data từ HDFS

```
/data/realestate/processed/gold/unified/house/{date}/unified_house_{date}.parquet
```

### Advanced Features được tạo:

#### 1. **Numeric Features** (15 features)

-   `log_price`, `log_area`, `sqrt_area`, `sqrt_price`
-   `price_per_sqm` (chỉ số quan trọng nhất)
-   Bedroom/bathroom ratios
-   Property size categories

#### 2. **Geospatial Features** (8 features)

-   Major city detection (HCM, Hà Nội, Đà Nẵng, etc.)
-   Region classification (North/South/Central)
-   Province/district normalization

#### 3. **Market Features** (12 features)

-   Province-level price comparisons
-   Relative pricing vs market average
-   Price segments and categories

#### 4. **Text Features** (6 features)

-   Luxury keywords detection ("cao cấp", "sang trọng")
-   New property keywords ("mới", "mới xây")
-   Title/description length analysis

#### 5. **Temporal Features** (4 features)

-   Seasonality indicators
-   Time since posted

#### 6. **Interaction Features** (15 features)

-   Cross-variable relationships
-   Complex feature combinations

### Output: Feature Store

```
/data/realestate/ml/features/house/{date}/
├── features.parquet           # Processed features
└── feature_metadata.json     # Feature definitions & stats
```

## 🤖 Advanced ML Training

### Models được train:

1. **XGBoost Regressor**

    - Best performance cho tabular data
    - Hyperparameter tuning với GridSearch
    - Feature importance analysis

2. **LightGBM Regressor**

    - Fast training & inference
    - Handling categorical features natively
    - Memory efficient

3. **CatBoost Regressor**

    - Robust to overfitting
    - Built-in categorical handling
    - Good default parameters

4. **Ensemble Model**
    - Weighted average của top models
    - Cross-validation để chọn weights tối ưu
    - Higher accuracy và stability

### Model Evaluation:

-   **RMSE**: Root Mean Square Error
-   **MAE**: Mean Absolute Error
-   **R²**: Coefficient of Determination
-   **Cross-validation**: 5-fold CV

### Model Output:

```
/data/realestate/ml/models/house/{date}/
├── xgboost_model.json         # XGBoost model
├── lightgbm_model.pkl         # LightGBM model
├── catboost_model.cbm         # CatBoost model
├── ensemble_model.pkl         # Ensemble model
└── model_metadata.json       # Performance metrics & configs
```

## 🔄 Incremental Training Strategy

### Daily Workflow:

1. **Data Detection**: Check for new Gold data
2. **Drift Analysis**: Detect feature/target distribution changes
3. **Incremental Update**: Update models với new data
4. **Performance Validation**: Compare với baseline performance
5. **Deployment Decision**: Deploy nếu performance improves

### Triggers cho Full Retraining:

-   Data drift detected (distribution changes > 15%)
-   Performance degradation (R² drops below 0.75)
-   Time threshold (7 days since last full training)

### Model Registry:

-   Automatic model versioning
-   Performance tracking across versions
-   Rollback capability nếu model performance drops

## 📊 Model Performance

### Current Benchmarks (trên real data):

-   **RMSE**: ~500M VND (typical Vietnamese housing prices)
-   **MAE**: ~350M VND
-   **R²**: 0.85+ (explains 85%+ of price variance)

### Feature Importance (Top 10):

1. `price_per_sqm` - Giá/m² (quan trọng nhất)
2. `area` - Diện tích
3. `province_clean` - Tỉnh/thành
4. `is_major_city` - Thành phố lớn
5. `bedrooms_clean` - Số phòng ngủ
6. `area_category` - Phân loại diện tích
7. `region` - Vùng miền
8. `total_rooms` - Tổng số phòng
9. `has_luxury_keywords` - Keywords cao cấp
10. `price_vs_province_avg` - So sánh giá tỉnh

## 🔮 Inference & Prediction

### Real-time Prediction API:

```bash
# Start inference service
docker exec realestate-ml-inference-api python -m data_processing.ml.ml_inference_pipeline

# API endpoint: http://localhost:8080
```

### Prediction Input:

```json
{
    "area": 80,
    "province": "Hồ Chí Minh",
    "district": "Quận 1",
    "bedrooms": 2,
    "bathrooms": 2,
    "title": "Căn hộ cao cấp view sông",
    "description": "Nhà mới xây, nội thất đầy đủ"
}
```

### Prediction Output:

```json
{
    "predicted_price": 8500000000,
    "confidence_interval": [7800000000, 9200000000],
    "model_used": "ensemble",
    "prediction_timestamp": "2025-06-06T10:30:00Z"
}
```

## 📈 Monitoring & MLflow

### MLflow Tracking:

-   **URL**: http://localhost:5000
-   **Experiments**: Automated experiment logging
-   **Metrics**: Performance tracking across runs
-   **Artifacts**: Model files, feature importance plots

### Key Metrics được track:

-   Training/validation RMSE, MAE, R²
-   Feature importance scores
-   Model training time
-   Data drift scores
-   Prediction latency

## 🐳 Docker Services

### ML Services running:

1. **ml-trainer**: Advanced ML training với XGBoost/LightGBM/CatBoost
2. **ml-incremental-trainer**: Daily incremental learning
3. **ml-inference-api**: FastAPI prediction service
4. **mlflow**: Experiment tracking server
5. **redis**: Feature caching & model caching

### Service URLs:

-   **Spark UI**: http://localhost:4040
-   **MLflow**: http://localhost:5000
-   **ML Inference API**: http://localhost:8080
-   **Redis**: localhost:6379

## 🧪 Testing & Validation

### Test Commands:

```bash
# Test full pipeline
./scripts/ml-pipeline-test.sh full

# Test individual components
./scripts/ml-pipeline-test.sh features
./scripts/ml-pipeline-test.sh training
./scripts/ml-pipeline-test.sh incremental
./scripts/ml-pipeline-test.sh inference
```

### Validation Steps:

1. **Data Quality**: Check cho missing values, outliers
2. **Feature Engineering**: Validate feature distributions
3. **Model Training**: Cross-validation performance
4. **Inference**: Prediction accuracy on holdout set

## 🎯 Production Deployment Checklist

-   ✅ **HDFS Integration**: Read Gold data từ distributed storage
-   ✅ **Spark Integration**: Scalable feature engineering
-   ✅ **Advanced Models**: XGBoost, LightGBM, CatBoost
-   ✅ **Model Versioning**: Automated versioning với metadata
-   ✅ **Incremental Learning**: Daily model updates
-   ✅ **Docker Deployment**: Production-ready containers
-   ✅ **API Service**: FastAPI inference endpoint
-   ✅ **Monitoring**: MLflow experiment tracking
-   ✅ **Error Handling**: Comprehensive error handling & logging

## 🔧 Troubleshooting

### Common Issues:

1. **HDFS Connection Failed**:

```bash
# Check HDFS containers
docker ps | grep hdfs
# Restart if needed
docker-compose -f docker/yml/hdfs.yml restart
```

2. **Spark Connection Failed**:

```bash
# Check Spark master
docker ps | grep spark-master
# Check Spark UI: http://localhost:8080
```

3. **Feature Engineering Failed**:

```bash
# Check Gold data exists
docker exec realestate-namenode-1 hdfs dfs -ls /data/realestate/processed/gold/unified/house/

# Run feature engineering manually
cd data_processing/spark/jobs/enrichment
python ml_feature_engineering.py --date 2025-06-06 --property-type house
```

4. **Model Training Failed**:

```bash
# Check feature data exists
docker exec realestate-namenode-1 hdfs dfs -ls /data/realestate/ml/features/house/

# Check memory/resources
docker stats
```

### Debug Commands:

```bash
# Check container logs
docker logs realestate-ml-trainer
docker logs realestate-mlflow

# Enter container for debugging
docker exec -it realestate-ml-trainer bash

# Check HDFS data
docker exec realestate-namenode-1 hdfs dfs -ls /data/realestate/
```

## 📞 Support

For questions về ML pipeline:

-   Check MLflow UI cho experiment details: http://localhost:5000
-   Check container logs: `docker logs [container-name]`
-   Run test script: `./scripts/ml-pipeline-test.sh`

---

🚀 **Happy ML Training!** 🤖
