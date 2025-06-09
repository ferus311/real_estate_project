# 🏠 Quy Trình Xử Lý Dữ Liệu Training Model Dự Đoán Giá Nhà

## 📋 Tổng Quan

Tài liệu này mô tả chi tiết **quy trình xử lý dữ liệu cụ thể** để training model dự đoán giá nhà bất động sản, bao gồm từ Gold Data đến Model đã được huấn luyện sẵn sàng prediction.

---

## 🔄 Luồng Xử Lý Dữ Liệu Training

```
📊 Gold Data (HDFS)           🎯 NGUỒN DỮ LIỆU ĐÃ ĐƯỢC LÀM SẠCH
       ↓
🔍 Data Quality Validation    🔍 KIỂM TRA CHẤT LƯỢNG & TÍNH TOÀN VẸN
       ↓
🔧 Feature Engineering        🛠️ TẠO CÁC FEATURES CHO ML
       ↓
📊 Data Preparation          📈 CHUẨN BỊ DỮ LIỆU CHO TRAINING
       ↓
🤖 Model Training Pipeline   🎯 TRAINING MULTIPLE MODELS
       ↓
💾 Model Registry            📦 LƯU TRỮ & PHIÊN BẢN MODEL
       ↓
🔮 Production Deployment     🚀 TRIỂN KHAI SẢN XUẤT
```

### 🎯 Luồng Dữ Liệu Chi Tiết

```
Raw Data → Bronze → Silver → Gold → ML Features → Trained Models → API Serving
                                  ↑____________TRAINING SCOPE____________↑
```

## 📊 Gold Data - Nguồn Dữ Liệu Training

### 🗂️ Cấu Trúc Dữ Liệu Gold

```
/data/realestate/processed/gold/unified/
├── apartment/               # 🏢 Căn hộ
│   └── 2025/01/06/
│       └── unified_apartment_20250106.parquet
├── house/                   # 🏠 Nhà phố
│   └── 2025/01/06/
│       └── unified_house_20250106.parquet
├── land/                    # 🌍 Đất nền
│   └── 2025/01/06/
│       └── unified_land_20250106.parquet
└── commercial/              # 🏪 Thương mại
    └── 2025/01/06/
        └── unified_commercial_20250106.parquet
```

### 📋 Schema Dữ Liệu Gold

```python
Gold Data Schema (Ready for ML):
├── id: string                    # Unique property ID
├── price: long                   # Target variable (VND)
├── area: double                  # Diện tích (m²)
├── bedroom: double               # Số phòng ngủ
├── bathroom: double              # Số phòng tắm
├── floor_count: double           # Số tầng
├── facade_width: double          # Mặt tiền (m)
├── road_width: double            # Đường vào (m)
├── city: string                  # Thành phố
├── district: string              # Quận/Huyện
├── ward: string                  # Phường/Xã
├── address: string               # Địa chỉ chi tiết
├── latitude: double              # Tọa độ GPS
├── longitude: double             # Tọa độ GPS
├── property_type: string         # Loại BDS
├── legal_status: string          # Tình trạng pháp lý
├── interior_status: string       # Tình trạng nội thất
├── house_direction: string       # Hướng nhà
├── description: string           # Mô tả chi tiết
├── crawl_date: date             # Ngày thu thập
├── source: string               # Nguồn dữ liệu
├── url: string                  # Link gốc
└── quality_score: double        # Điểm chất lượng dữ liệu
```

---

## 🔍 Bước 1: Data Quality Validation

### 🎯 Mục Đích

Kiểm tra chất lượng và tính toàn vẹn của Gold Data trước khi training

### 🔧 Quy Trình Validation

```python
# File: data_processing/spark/jobs/enrichment/ml_feature_engineering.py
def validate_training_data(df: DataFrame) -> Dict[str, Any]:
    """
    Validates data quality for ML training
    """
    validation_results = {
        'total_records': df.count(),
        'missing_target': df.filter(col('price').isNull()).count(),
        'missing_features': {},
        'outliers': {},
        'quality_flags': {}
    }

    # 1. Target Variable Validation
    price_stats = df.select(
        count('price').alias('valid_prices'),
        min('price').alias('min_price'),
        max('price').alias('max_price'),
        avg('price').alias('avg_price')
    ).collect()[0]

    # 2. Feature Completeness Check
    critical_features = ['area', 'city', 'district', 'property_type']
    for feature in critical_features:
        missing_count = df.filter(col(feature).isNull()).count()
        validation_results['missing_features'][feature] = missing_count

    # 3. Outlier Detection
    # Price outliers (beyond 3 standard deviations)
    price_mean = df.select(avg('price')).collect()[0][0]
    price_std = df.select(stddev('price')).collect()[0][0]
    outlier_threshold = price_mean + (3 * price_std)

    outliers = df.filter(col('price') > outlier_threshold).count()
    validation_results['outliers']['price'] = outliers

    return validation_results
```

### 📊 Quality Metrics

| Metric           | Threshold | Action      |
| ---------------- | --------- | ----------- |
| Missing Price    | < 5%      | ✅ Continue |
| Missing Area     | < 10%     | ✅ Continue |
| Missing Location | < 15%     | ⚠️ Warning  |
| Price Outliers   | < 2%      | ✅ Continue |
| Data Freshness   | < 7 days  | ✅ Continue |

---

## 🔧 Bước 2: Feature Engineering Pipeline

### 🎯 Mục Đích

Chuyển đổi Gold Data thành **60+ ML Features** phong phú và có ý nghĩa

### 📈 Categories Features được tạo

#### 1. **Basic Features** (15 features)

```python
# Numeric transformations
price_log = log(price + 1)                    # Log transformation
area_log = log(area + 1)                      # Log transformation
price_per_sqm = price / area                  # Giá trên m²
price_per_bedroom = price / bedroom           # Giá trên phòng ngủ
bedroom_bathroom_ratio = bedroom / bathroom   # Tỷ lệ phòng

# Boolean features
has_elevator = facade_width > 8               # Có thang máy
is_corner_lot = road_width > 6               # Đất góc
is_large_property = area > 200               # BDS lớn
```

#### 2. **Geospatial Features** (8 features)

```python
# Location encoding
city_encoded = StringIndexer.fit('city')
district_encoded = StringIndexer.fit('district')
province_normalized = standardize_province_names()

# Distance calculations (if coordinates available)
distance_to_center = calculate_distance_to_city_center()
proximity_to_main_road = estimate_road_proximity()
```

#### 3. **Market Features** (12 features)

```python
# Market analysis per location
avg_price_by_location = Window.partitionBy('city', 'district').avg('price')
price_deviation_from_market = (price - avg_price_by_location) / avg_price_by_location
price_percentile_in_location = percent_rank().over(location_window)

# Supply density
supply_density = count().over(location_window)
price_volatility = stddev('price').over(location_window)
```

#### 4. **Temporal Features** (4 features)

```python
# Time components
year = year('crawl_date')
month = month('crawl_date')
quarter = quarter('crawl_date')
season = when(month.isin(12,1,2), 'winter')
         .when(month.isin(3,4,5), 'spring')
         .when(month.isin(6,7,8), 'summer')
         .otherwise('autumn')

# Recency
days_since_crawl = datediff(current_date(), 'crawl_date')
```

#### 5. **Text Features** (6 features)

```python
# Description analysis
description_length = length('description')
description_word_count = size(split('description', ' '))
has_balcony = description.contains('ban công|balcony')
has_parking = description.contains('chỗ đỗ xe|garage|parking')
has_garden = description.contains('sân vườn|garden')
description_richness = description_length / 100  # Quality metric
```

#### 6. **Interaction Features** (15 features)

```python
# Feature combinations
area_location_interaction = area * city_encoded
price_segment = when(price_percentile_in_location < 0.33, 'low')
           .when(price_percentile_in_location < 0.67, 'medium')
           .otherwise('high')

# Property value indicators
luxury_indicator = (price_per_sqm > luxury_threshold) & (area > 150)
investment_potential = price_deviation_from_market < -0.2  # Undervalued
```

### 🔄 Feature Engineering Flow

```python
# File: data_processing/spark/jobs/enrichment/ml_feature_engineering.py
def run_feature_engineering_pipeline(spark, input_date, property_type):
    """
    Complete feature engineering pipeline
    """
    # 1. Load validated Gold data
    gold_path = f"/data/realestate/processed/gold/unified/{property_type}"
    gold_df = spark.read.parquet(f"{gold_path}/{input_date}")

    # 2. Create basic features
    features_df = create_basic_features(gold_df)

    # 3. Add geospatial features
    features_df = add_geospatial_features(features_df)

    # 4. Add market features
    features_df = add_market_features(features_df)

    # 5. Add temporal features
    features_df = add_temporal_features(features_df)

    # 6. Add text features
    features_df = add_text_features(features_df)

    # 7. Add interaction features
    features_df = add_interaction_features(features_df)

    # 8. Save to Feature Store
    output_path = f"/data/realestate/ml/features/{property_type}/{input_date}"
    features_df.write.mode('overwrite').parquet(output_path)

    return features_df
```

### 📊 Feature Store Output

```
/data/realestate/ml/features/{property_type}/{date}/
├── features.parquet              # Processed features (60+ columns)
├── feature_metadata.json        # Feature definitions & statistics
└── feature_importance.json      # Feature importance from previous runs
```

│ │ ├── xgboost/
│ │ ├── lightgbm/
│ │ ├── catboost/
│ │ └── ensemble/
│ └── predictions/ # Prediction results
└── 📁 analytics/ # Analysis outputs
├── model_performance/
├── feature_importance/
└── market_insights/

````

### 📅 Chính Sách Lưu Trữ Dữ Liệu

| **Loại Dữ Liệu** | **Retention Period** | **Partitioning** | **Mục Đích** |
|-------------------|---------------------|------------------|--------------|
| 🥉 **Bronze** | 7 ngày | `yyyy/mm/dd` | Raw data temporary storage |
| 🥈 **Silver** | 30 ngày | `yyyy/mm/dd` | Cleaned data for analysis |
| 🥇 **Gold** | 90 ngày | `property_type/yyyy/mm/dd` | Business-ready data |
| 🔧 **ML Features** | 60 ngày | `property_type/yyyy-mm-dd` | Training data |
| 🤖 **Models** | 180 ngày | `model_name/version/` | Model artifacts |
| 📊 **Predictions** | 30 ngày | `yyyy/mm/dd/property_type` | Inference results |

### 🔄 Data Lifecycle Management

```python
# Daily Cleanup Process
def cleanup_old_data():
    """
    Xóa dữ liệu cũ theo chính sách retention
    Chạy hàng ngày lúc 2:00 AM
    """
    # Bronze: Xóa > 7 ngày
    remove_files_older_than("/data/realestate/processed/bronze", days=7)

    # Silver: Xóa > 30 ngày
    remove_files_older_than("/data/realestate/processed/silver", days=30)

    # Gold: Xóa > 90 ngày
    remove_files_older_than("/data/realestate/processed/gold", days=90)

    # ML Features: Xóa > 60 ngày
    remove_files_older_than("/data/realestate/ml/features", days=60)

    # Models: Giữ lại 3 models tốt nhất, xóa models > 180 ngày
    cleanup_model_registry(keep_best=3, max_age_days=180)
````

---

## 🔧 Logic Xử Lý Dữ Liệu Mỗi Lần Chạy

### 🕒 Scheduling Strategy

| **Frequency** | **Schedule**        | **Scope**       | **Purpose**                      |
| ------------- | ------------------- | --------------- | -------------------------------- |
| **Daily**     | 06:00 AM            | Incremental     | Training với data mới trong 24h  |
| **Weekly**    | Sunday 02:00 AM     | Full Retraining | Retrain toàn bộ với data 30 ngày |
| **Monthly**   | 1st Sunday 01:00 AM | Model Refresh   | Evaluation và model selection    |

### 📋 Daily Processing Logic

```python
def daily_ml_workflow(execution_date):
    """
    Logic xử lý hàng ngày cho ML pipeline

    Input: execution_date (yyyy-mm-dd)
    Output: Updated models và predictions
    """

    # 1️⃣ DATA VALIDATION
    data_quality_check = check_gold_data_quality(execution_date)
    if not data_quality_check.is_valid:
        raise ValueError("Data quality check failed")

    # 2️⃣ FEATURE ENGINEERING
    features_df = create_ml_features(
        gold_data_path=f"/data/realestate/processed/gold/unified/{property_type}/{execution_date}/",
        lookback_days=30,  # Sử dụng data 30 ngày gần nhất
        feature_config=FEATURE_CONFIG
    )

    # 3️⃣ INCREMENTAL TRAINING
    for property_type in ['apartment', 'house', 'land']:
        updated_model = incremental_training(
            property_type=property_type,
            new_features=features_df,
            base_model_path=f"/data/realestate/ml/models/current/{property_type}/",
            learning_rate=0.1
        )

        # 4️⃣ MODEL VALIDATION
        validation_results = validate_model_performance(
            model=updated_model,
            validation_data=features_df.sample(0.2),
            metrics=['mae', 'rmse', 'mape']
        )

        # 5️⃣ MODEL DEPLOYMENT (nếu performance tốt hơn)
        if validation_results.mae < current_best_mae:
            deploy_model_to_registry(
                model=updated_model,
                property_type=property_type,
                version=f"daily_{execution_date}",
                metrics=validation_results
            )

    # 6️⃣ PREDICTION GENERATION
    generate_daily_predictions(execution_date)

    # 7️⃣ PERFORMANCE MONITORING
    log_model_performance_metrics(execution_date)
```

### 📊 Data Flow trong mỗi lần chạy

```
🌅 Bắt đầu (06:00 AM)
    ↓
📊 1. Check Gold Data (Last 24h)
    ├─ ✅ Volume: Min 100 records/property_type
    ├─ ✅ Quality: 95% complete records
    └─ ✅ Freshness: Data within 24h
    ↓
🔧 2. Feature Engineering (30 min)
    ├─ Load Gold data (30 days window)
    ├─ Apply feature transformations (60+ features)
    ├─ Create time-based features
    └─ Save to /ml/features/{property_type}/{date}/
    ↓
🤖 3. Incremental Training (60 min)
    ├─ Load current best models
    ├─ Train with new features
    ├─ Validate performance
    └─ Update if improved
    ↓
🔮 4. Generate Predictions (15 min)
    ├─ Predict for new listings
    ├─ Update market insights
    └─ Cache to Redis
    ↓
📈 5. Performance Monitoring (5 min)
    ├─ Log metrics to MLflow
    ├─ Generate alerts if needed
    └─ Update dashboards
    ↓
🎯 Hoàn thành (08:00 AM)
```

---

## 🎯 Chiến Lược Training và Lưu Trữ Best Models

### 🏆 Model Selection Strategy

Hệ thống lưu trữ **3 models tốt nhất** cho mỗi loại bất động sản:

```python
BEST_MODELS_CONFIG = {
    "selection_criteria": {
        "primary_metric": "mae",          # Mean Absolute Error
        "secondary_metrics": ["rmse", "mape", "r2"],
        "business_metrics": ["prediction_accuracy", "market_coverage"]
    },
    "model_types": {
        "ensemble": {"weight": 0.4, "priority": 1},    # Ưu tiên cao nhất
        "xgboost": {"weight": 0.25, "priority": 2},
        "lightgbm": {"weight": 0.25, "priority": 3},
        "catboost": {"weight": 0.1, "priority": 4}
    },
    "storage_strategy": {
        "keep_best": 3,                   # Giữ 3 models tốt nhất
        "archive_period": 180,            # Archive after 180 days
        "backup_frequency": "weekly"      # Weekly backups
    }
}
```

### 📦 Model Registry Structure

```
/data/realestate/ml/models/
├── 📁 current/                    # Models đang sử dụng
│   ├── apartment/
│   │   ├── best_model_1/         # MAE: 0.085 (Ensemble)
│   │   ├── best_model_2/         # MAE: 0.092 (XGBoost)
│   │   └── best_model_3/         # MAE: 0.098 (LightGBM)
│   ├── house/
│   └── land/
├── 📁 archive/                    # Models cũ
│   ├── 2024-01/
│   ├── 2024-02/
│   └── 2024-03/
└── 📁 experiments/               # Experimental models
    ├── hyperparameter_tuning/
    ├── feature_selection/
    └── architecture_tests/
```

### 🔄 Model Update Process

```python
class ModelRegistryManager:
    """
    Quản lý registry và deployment của models
    """

    def evaluate_and_update_best_models(self, new_model, property_type):
        """
        Đánh giá model mới và cập nhật top 3 nếu cần
        """
        current_best_models = self.load_current_best_models(property_type)

        # Evaluate new model
        new_model_metrics = self.evaluate_model(new_model)

        # Compare with current best
        all_models = current_best_models + [new_model]
        ranked_models = self.rank_models_by_performance(all_models)

        # Keep top 3
        new_best_models = ranked_models[:3]

        # Update registry if changed
        if new_best_models != current_best_models:
            self.update_model_registry(property_type, new_best_models)
            self.send_notification(f"Best models updated for {property_type}")

    def deploy_model(self, model_id, property_type, deployment_type="blue_green"):
        """
        Deploy model với strategy blue-green
        """
        if deployment_type == "blue_green":
            # Deploy to staging environment first
            self.deploy_to_staging(model_id, property_type)

            # Run validation tests
            validation_results = self.run_deployment_validation(model_id)

            if validation_results["success"]:
                # Switch traffic from blue to green
                self.switch_traffic(model_id, property_type)
                self.log_deployment_success(model_id)
            else:
                self.rollback_deployment(model_id, property_type)
```

---

## 🔄 Bước 3: Data Preparation (Chuẩn Bị Dữ Liệu Training)

### 📊 Train/Validation/Test Split Strategy

```python
def create_temporal_splits(gold_data, feature_data, config):
    """
    Chia dữ liệu theo thời gian để tránh data leakage
    """
    # Sort by listing_date để đảm bảo temporal order
    combined_data = gold_data.join(feature_data, ["listing_id"], "inner") \
                            .orderBy("listing_date")

    # Calculate split dates
    total_records = combined_data.count()
    train_end_date = "2024-01-31"     # 70% data (14 tháng đầu)
    val_end_date = "2024-03-31"       # 15% data (2 tháng)
    # Test data: 2024-04-01 trở đi     # 15% data (1.5 tháng)

    # Create splits
    train_data = combined_data.filter(f"listing_date <= '{train_end_date}'")
    val_data = combined_data.filter(f"listing_date > '{train_end_date}' AND listing_date <= '{val_end_date}'")
    test_data = combined_data.filter(f"listing_date > '{val_end_date}'")

    print(f"📊 Data Split Summary:")
    print(f"   Training: {train_data.count():,} records ({train_data.count()/total_records*100:.1f}%)")
    print(f"   Validation: {val_data.count():,} records ({val_data.count()/total_records*100:.1f}%)")
    print(f"   Test: {test_data.count():,} records ({test_data.count()/total_records*100:.1f}%)")

    return train_data, val_data, test_data
```

### 🏠 Property Type Stratification

```python
def stratify_by_property_type(train_data, val_data, test_data):
    """
    Đảm bảo distribution đồng đều cho từng loại BDS
    """
    property_distribution = {
        "apartment": {"train": 0.65, "val": 0.20, "test": 0.15},
        "house": {"train": 0.70, "val": 0.15, "test": 0.15},
        "land": {"train": 0.75, "val": 0.15, "test": 0.10}  # Ít data hơn cho land
    }

    stratified_datasets = {}

    for property_type in ["apartment", "house", "land"]:
        # Filter by property type
        train_prop = train_data.filter(f"property_type = '{property_type}'")
        val_prop = val_data.filter(f"property_type = '{property_type}'")
        test_prop = test_data.filter(f"property_type = '{property_type}'")

        # Apply additional stratification if needed
        if property_type == "apartment":
            # Stratify by district for apartments (urban distribution)
            train_prop = stratify_by_district(train_prop, min_samples_per_district=100)
            val_prop = stratify_by_district(val_prop, min_samples_per_district=20)

        stratified_datasets[property_type] = {
            "train": train_prop,
            "val": val_prop,
            "test": test_prop
        }

        print(f"🏠 {property_type.title()} Distribution:")
        print(f"   Train: {train_prop.count():,}")
        print(f"   Val: {val_prop.count():,}")
        print(f"   Test: {test_prop.count():,}")

    return stratified_datasets
```

### 🧹 Data Preprocessing Pipeline

```python
class DataPreprocessor:
    """
    Preprocessing pipeline cho training data
    """

    def __init__(self, config):
        self.config = config
        self.scalers = {}
        self.encoders = {}
        self.feature_selectors = {}

    def preprocess_features(self, train_data, val_data, test_data, property_type):
        """
        Apply full preprocessing pipeline
        """
        print(f"🧹 Preprocessing features for {property_type}...")

        # 1. Handle missing values
        train_clean = self.handle_missing_values(train_data, property_type, fit=True)
        val_clean = self.handle_missing_values(val_data, property_type, fit=False)
        test_clean = self.handle_missing_values(test_data, property_type, fit=False)

        # 2. Feature scaling
        train_scaled = self.scale_numerical_features(train_clean, property_type, fit=True)
        val_scaled = self.scale_numerical_features(val_clean, property_type, fit=False)
        test_scaled = self.scale_numerical_features(test_clean, property_type, fit=False)

        # 3. Categorical encoding
        train_encoded = self.encode_categorical_features(train_scaled, property_type, fit=True)
        val_encoded = self.encode_categorical_features(val_scaled, property_type, fit=False)
        test_encoded = self.encode_categorical_features(test_scaled, property_type, fit=False)

        # 4. Feature selection
        selected_features = self.select_features(train_encoded, property_type, fit=True)
        train_final = train_encoded.select(selected_features)
        val_final = val_encoded.select(selected_features)
        test_final = test_encoded.select(selected_features)

        return train_final, val_final, test_final

    def handle_missing_values(self, data, property_type, fit=False):
        """
        Xử lý missing values với strategy khác nhau cho từng feature type
        """
        missing_strategies = {
            # Numerical features
            "area": "median",           # Diện tích → median theo district
            "price_per_sqm": "median",  # Giá/m2 → median theo district
            "age": "mean",              # Tuổi nhà → mean
            "distance_*": "median",     # Khoảng cách → median

            # Categorical features
            "legal_document": "mode",   # Giấy tờ → mode
            "interior": "unknown",      # Nội thất → "unknown"
            "orientation": "unknown",   # Hướng nhà → "unknown"

            # Boolean features
            "has_*": False,            # Has features → False
            "is_*": False,             # Is features → False
        }

        # Apply imputation
        for feature, strategy in missing_strategies.items():
            if "*" in feature:
                # Handle wildcard patterns
                matching_cols = [col for col in data.columns if feature.replace("*", "") in col]
                for col in matching_cols:
                    data = self.impute_column(data, col, strategy, property_type, fit)
            else:
                data = self.impute_column(data, feature, strategy, property_type, fit)

        return data

    def scale_numerical_features(self, data, property_type, fit=False):
        """
        Scale numerical features using RobustScaler
        """
        numerical_features = [
            "area", "price_per_sqm", "age", "floor", "bedrooms", "bathrooms",
            "distance_to_center", "distance_to_metro", "distance_to_school",
            "district_avg_price", "market_velocity", "listing_frequency",
            "temporal_price_trend", "seasonal_factor"
        ]

        scaler_key = f"{property_type}_numerical"

        if fit:
            # Fit scaler on training data
            from sklearn.preprocessing import RobustScaler
            self.scalers[scaler_key] = RobustScaler()

            # Convert to pandas for fitting
            train_pandas = data.select(numerical_features).toPandas()
            self.scalers[scaler_key].fit(train_pandas)

        # Transform data
        pandas_data = data.select(numerical_features).toPandas()
        scaled_data = self.scalers[scaler_key].transform(pandas_data)

        # Convert back to Spark DataFrame
        scaled_df = spark.createDataFrame(
            pd.DataFrame(scaled_data, columns=numerical_features)
        )

        # Join with non-numerical features
        non_numerical_cols = [col for col in data.columns if col not in numerical_features]
        non_numerical_data = data.select(non_numerical_cols)

        return non_numerical_data.join(scaled_df, on=["listing_id"])
```

---

## 🚀 Bước 4: Model Training Pipeline (Pipeline Training Models)

### 🎯 Multi-Model Training Strategy

```python
class ModelTrainingPipeline:
    """
    Pipeline training multiple models song song
    """

    def __init__(self, config):
        self.config = config
        self.models = self.initialize_models()
        self.hyperparameter_grids = self.setup_hyperparameter_grids()

    def train_all_models(self, train_data, val_data, property_type):
        """
        Train tất cả models cho một property type
        """
        print(f"🚀 Training models for {property_type}...")

        trained_models = {}
        training_history = {}

        # Train base models
        for model_name in ["xgboost", "lightgbm", "catboost", "random_forest"]:
            print(f"   🔥 Training {model_name}...")

            # Hyperparameter tuning
            best_params = self.hyperparameter_tuning(
                model_name, train_data, val_data, property_type
            )

            # Train final model with best params
            model = self.train_single_model(
                model_name, train_data, val_data, best_params
            )

            # Evaluate performance
            performance = self.evaluate_model_performance(model, val_data)

            trained_models[model_name] = {
                "model": model,
                "params": best_params,
                "performance": performance
            }

            training_history[model_name] = {
                "training_time": model.training_time,
                "feature_importance": model.feature_importance,
                "validation_scores": model.validation_scores
            }

            print(f"   ✅ {model_name} MAE: {performance['mae']:.4f}")

        # Train ensemble model
        print(f"   🎯 Training ensemble model...")
        ensemble_model = self.train_ensemble_model(trained_models, val_data)

        trained_models["ensemble"] = {
            "model": ensemble_model,
            "params": {"base_models": list(trained_models.keys())},
            "performance": self.evaluate_model_performance(ensemble_model, val_data)
        }

        return trained_models, training_history

    def hyperparameter_tuning(self, model_name, train_data, val_data, property_type):
        """
        Hyperparameter tuning với Optuna
        """
        import optuna

        def objective(trial):
            # Generate hyperparameters based on model type
            if model_name == "xgboost":
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
                    "max_depth": trial.suggest_int("max_depth", 3, 10),
                    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
                    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                    "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                }
            elif model_name == "lightgbm":
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
                    "max_depth": trial.suggest_int("max_depth", 3, 10),
                    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
                    "num_leaves": trial.suggest_int("num_leaves", 10, 100),
                    "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
                }
            elif model_name == "catboost":
                params = {
                    "iterations": trial.suggest_int("iterations", 100, 1000),
                    "depth": trial.suggest_int("depth", 4, 10),
                    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
                    "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1, 10),
                }

            # Train model with suggested params
            model = self.train_single_model(model_name, train_data, val_data, params)

            # Return validation MAE (minimize)
            val_predictions = model.predict(val_data)
            mae = mean_absolute_error(val_data["price"], val_predictions)

            return mae

        # Run optimization
        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=50)

        return study.best_params

    def train_single_model(self, model_name, train_data, val_data, params):
        """
        Train một model với parameters cụ thể
        """
        if model_name == "xgboost":
            import xgboost as xgb

            # Prepare data
            X_train = train_data.drop(["listing_id", "price"], axis=1)
            y_train = train_data["price"]
            X_val = val_data.drop(["listing_id", "price"], axis=1)
            y_val = val_data["price"]

            # Create model
            model = xgb.XGBRegressor(**params, random_state=42)

            # Train with early stopping
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                early_stopping_rounds=50,
                verbose=False
            )

        elif model_name == "lightgbm":
            import lightgbm as lgb

            X_train = train_data.drop(["listing_id", "price"], axis=1)
            y_train = train_data["price"]
            X_val = val_data.drop(["listing_id", "price"], axis=1)
            y_val = val_data["price"]

            model = lgb.LGBMRegressor(**params, random_state=42)
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(50)]
            )

        elif model_name == "catboost":
            from catboost import CatBoostRegressor

            X_train = train_data.drop(["listing_id", "price"], axis=1)
            y_train = train_data["price"]
            X_val = val_data.drop(["listing_id", "price"], axis=1)
            y_val = val_data["price"]

            model = CatBoostRegressor(**params, random_state=42, verbose=False)
            model.fit(
                X_train, y_train,
                eval_set=(X_val, y_val),
                early_stopping_rounds=50
            )

        return model

    def train_ensemble_model(self, base_models, val_data):
        """
        Train ensemble model từ base models
        """
        from sklearn.ensemble import VotingRegressor
        from sklearn.linear_model import Ridge

        # Get predictions from base models
        base_predictions = {}
        for model_name, model_info in base_models.items():
            predictions = model_info["model"].predict(val_data.drop(["listing_id", "price"], axis=1))
            base_predictions[model_name] = predictions

        # Create meta-features
        meta_features = pd.DataFrame(base_predictions)
        meta_target = val_data["price"]

        # Train meta-learner
        meta_learner = Ridge(alpha=1.0)
        meta_learner.fit(meta_features, meta_target)

        # Create ensemble model
        ensemble = EnsembleModel(base_models, meta_learner)

        return ensemble

class EnsembleModel:
    """
    Ensemble model wrapper
    """

    def __init__(self, base_models, meta_learner):
        self.base_models = base_models
        self.meta_learner = meta_learner

    def predict(self, X):
        # Get predictions from base models
        base_predictions = {}
        for model_name, model_info in self.base_models.items():
            predictions = model_info["model"].predict(X)
            base_predictions[model_name] = predictions

        # Create meta-features
        meta_features = pd.DataFrame(base_predictions)

        # Get final prediction from meta-learner
        final_predictions = self.meta_learner.predict(meta_features)

        return final_predictions
```

### 📊 Training Monitoring & Logging

```python
class TrainingMonitor:
    """
    Monitor training progress và log metrics
    """

    def __init__(self, experiment_name):
        import mlflow
        self.experiment_name = experiment_name
        mlflow.set_experiment(experiment_name)

    def log_training_run(self, model_name, property_type, params, metrics, artifacts):
        """
        Log training run to MLflow
        """
        import mlflow

        with mlflow.start_run(run_name=f"{model_name}_{property_type}"):
            # Log parameters
            mlflow.log_params(params)

            # Log metrics
            mlflow.log_metrics(metrics)

            # Log model
            mlflow.sklearn.log_model(
                artifacts["model"],
                f"{model_name}_model"
            )

            # Log feature importance
            if "feature_importance" in artifacts:
                mlflow.log_artifact(artifacts["feature_importance"], "feature_importance.png")

            # Log training curves
            if "training_curves" in artifacts:
                mlflow.log_artifact(artifacts["training_curves"], "training_curves.png")

            # Log data drift reports
            if "data_drift_report" in artifacts:
                mlflow.log_artifact(artifacts["data_drift_report"], "data_drift_report.html")

    def create_training_dashboard(self, training_history):
        """
        Tạo dashboard theo dõi training
        """
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Training MAE", "Validation MAE", "Training Time", "Feature Importance"),
            specs=[[{"type": "scatter"}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )

        # Training/Validation MAE over time
        for model_name, history in training_history.items():
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(history["train_mae"]))),
                    y=history["train_mae"],
                    name=f"{model_name}_train",
                    mode="lines"
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Scatter(
                    x=list(range(len(history["val_mae"]))),
                    y=history["val_mae"],
                    name=f"{model_name}_val",
                    mode="lines"
                ),
                row=1, col=2
            )

        # Training time comparison
        model_names = list(training_history.keys())
        training_times = [training_history[name]["training_time"] for name in model_names]

        fig.add_trace(
            go.Bar(x=model_names, y=training_times, name="Training Time"),
            row=2, col=1
        )

        fig.update_layout(
            title="Model Training Dashboard",
            height=800
        )

        return fig
```

---

## 📈 Bước 5: Model Evaluation & Selection (Đánh Giá & Lựa Chọn Model)

### 🎯 Comprehensive Model Evaluation

```python
class ModelEvaluator:
    """
    Đánh giá toàn diện performance của models
    """

    def __init__(self, config):
        self.config = config
        self.evaluation_metrics = [
            "mae", "rmse", "mape", "r2", "median_ae", "max_error"
        ]

    def evaluate_all_models(self, trained_models, test_data, property_type):
        """
        Evaluate tất cả trained models trên test set
        """
        print(f"📈 Evaluating models for {property_type}...")

        evaluation_results = {}

        for model_name, model_info in trained_models.items():
            print(f"   🔍 Evaluating {model_name}...")

            # Get predictions
            model = model_info["model"]
            X_test = test_data.drop(["listing_id", "price"], axis=1)
            y_test = test_data["price"]
            predictions = model.predict(X_test)

            # Calculate metrics
            metrics = self.calculate_comprehensive_metrics(y_test, predictions)

            # Price range analysis
            price_range_metrics = self.analyze_by_price_ranges(y_test, predictions)

            # Geographic analysis
            geographic_metrics = self.analyze_by_geography(
                test_data, predictions, property_type
            )

            # Temporal analysis
            temporal_metrics = self.analyze_by_time_periods(
                test_data, predictions
            )

            evaluation_results[model_name] = {
                "overall_metrics": metrics,
                "price_range_metrics": price_range_metrics,
                "geographic_metrics": geographic_metrics,
                "temporal_metrics": temporal_metrics,
                "predictions": predictions.tolist(),
                "residuals": (y_test - predictions).tolist()
            }

            print(f"   ✅ {model_name} Results:")
            print(f"      MAE: {metrics['mae']:.4f}")
            print(f"      RMSE: {metrics['rmse']:.4f}")
            print(f"      MAPE: {metrics['mape']:.2f}%")
            print(f"      R²: {metrics['r2']:.4f}")

        return evaluation_results

    def calculate_comprehensive_metrics(self, y_true, y_pred):
        """
        Tính toán các metrics đánh giá comprehensive
        """
        from sklearn.metrics import (
            mean_absolute_error, mean_squared_error,
            r2_score, median_absolute_error, max_error
        )
        import numpy as np

        metrics = {
            "mae": mean_absolute_error(y_true, y_pred),
            "rmse": np.sqrt(mean_squared_error(y_true, y_pred)),
            "mape": np.mean(np.abs((y_true - y_pred) / y_true)) * 100,
            "r2": r2_score(y_true, y_pred),
            "median_ae": median_absolute_error(y_true, y_pred),
            "max_error": max_error(y_true, y_pred),

            # Business metrics
            "predictions_within_10pct": np.mean(np.abs((y_true - y_pred) / y_true) <= 0.1) * 100,
            "predictions_within_20pct": np.mean(np.abs((y_true - y_pred) / y_true) <= 0.2) * 100,

            # Bias metrics
            "mean_residual": np.mean(y_true - y_pred),
            "residual_std": np.std(y_true - y_pred),
        }

        return metrics

    def analyze_by_price_ranges(self, y_true, y_pred):
        """
        Phân tích performance theo từng price range
        """
        import pandas as pd

        df = pd.DataFrame({
            "actual": y_true,
            "predicted": y_pred,
            "error": np.abs(y_true - y_pred),
            "error_pct": np.abs((y_true - y_pred) / y_true) * 100
        })

        # Define price ranges (in billions VND)
        price_ranges = [
            ("under_2b", 0, 2),      # Dưới 2 tỷ
            ("2b_to_5b", 2, 5),      # 2-5 tỷ
            ("5b_to_10b", 5, 10),    # 5-10 tỷ
            ("10b_to_20b", 10, 20),  # 10-20 tỷ
            ("over_20b", 20, 1000)   # Trên 20 tỷ
        ]

        range_metrics = {}

        for range_name, min_price, max_price in price_ranges:
            mask = (df["actual"] >= min_price) & (df["actual"] < max_price)
            range_data = df[mask]

            if len(range_data) > 0:
                range_metrics[range_name] = {
                    "count": len(range_data),
                    "mae": range_data["error"].mean(),
                    "mape": range_data["error_pct"].mean(),
                    "within_10pct": (range_data["error_pct"] <= 10).mean() * 100,
                    "within_20pct": (range_data["error_pct"] <= 20).mean() * 100
                }
```
