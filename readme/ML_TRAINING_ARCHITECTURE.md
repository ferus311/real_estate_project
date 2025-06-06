# Kiến Trúc ML Training Pipeline - Real Estate Project

## Tổng quan Luồng dữ liệu hiện tại

```
Raw Data (Crawled) → Bronze (Standardized) → Silver (Cleaned) → Gold (Unified)
```

## Thiết kế ML Pipeline Architecture

### 1. DATA FLOW ARCHITECTURE

```
Gold Data (HDFS)
    ↓
Data Quality Validation
    ↓
Feature Engineering Pipeline
    ↓
Data Preparation & Splitting
    ↓
Model Training Pipeline
    ↓
Model Evaluation & Validation
    ↓
Model Registry & Versioning
    ↓
Model Deployment (Production)
```

### 2. CHI TIẾT CÁC TẦNG XẢLÝ

#### 2.1 GOLD DATA INPUT LAYER

**Nguồn:** `/hdfs/gold/realestate/{yyyy-mm-dd}/unified_properties.parquet`

**Đặc điểm Gold Data:**

-   Đã unified từ nhiều nguồn (Batdongsan, Chotot, etc.)
-   Có schema chuẩn hóa
-   Đã qua basic cleaning ở Silver layer
-   Partitioned theo ngày

**Schema dự kiến:**

```
unified_properties:
├── property_id: string
├── source: string (batdongsan/chotot)
├── title: string
├── price_cleaned: double
├── area_cleaned: double
├── bedrooms_cleaned: int
├── bathrooms_cleaned: int
├── city: string
├── district: string
├── property_type: string
├── description: string
├── latitude: double
├── longitude: double
├── crawl_date: date
├── quality_flags: struct
└── enrichment_data: struct
```

#### 2.2 DATA QUALITY VALIDATION LAYER

**Mục đích:** Kiểm tra chất lượng data trước khi train

**Các kiểm tra:**

-   **Completeness:** % missing values cho features quan trọng
-   **Validity:** Range checks (price > 0, area > 0, etc.)
-   **Consistency:** Cross-field validation
-   **Freshness:** Data recency checks
-   **Distribution:** Statistical outlier detection

**Output:**

-   Quality report
-   Filtered dataset (chỉ data đạt quality threshold)

#### 2.3 FEATURE ENGINEERING PIPELINE

**Mục đích:** Tạo features phong phú từ raw Gold data

**Feature Categories:**

**A. Numeric Features:**

-   `price_log`: log transformation of price
-   `area_log`: log transformation of area
-   `price_per_sqm`: price density
-   `price_per_bedroom`: room value metric
-   `bedroom_bathroom_ratio`: layout efficiency

**B. Temporal Features:**

-   `year`, `month`, `quarter`, `season`: time components
-   `days_since_crawl`: recency metric
-   `is_weekend`: timing feature

**C. Location Features:**

-   `city_district`: combined location
-   `location_price_rank`: price ranking by location
-   `distance_to_center`: proximity feature (if coordinates available)

**D. Market Features:**

-   `avg_price_by_location`: local market average
-   `price_deviation_from_market`: relative positioning
-   `supply_density`: properties count by area
-   `price_volatility`: local price variance

**E. Quality Features:**

-   `data_completeness_score`: data quality metric
-   `source_reliability`: source-based scoring
-   `description_richness`: text content quality

**F. Interaction Features:**

-   `area_location_interaction`: size-location combination
-   `price_segment`: high/medium/low by location

#### 2.4 DATA PREPARATION LAYER

**Mục đích:** Chuẩn bị data cho training

**Các bước:**

**A. Feature Selection:**

-   Correlation analysis
-   Feature importance ranking
-   Multicollinearity detection
-   Business logic filtering

**B. Data Splitting Strategy:**

```
Training Set (70%): Oldest data → Recent-2months
Validation Set (15%): Recent-2months → Recent-1month
Test Set (15%): Recent-1month → Latest
```

**C. Data Preprocessing:**

-   Categorical encoding (One-hot/Label encoding)
-   Numerical scaling (StandardScaler/MinMaxScaler)
-   Missing value imputation
-   Outlier treatment

#### 2.5 MODEL TRAINING PIPELINE

**Mục đích:** Train multiple model variants và chọn best model

**A. Model Types:**

```
Primary Models:
├── RandomForestRegressor
├── GradientBoostingRegressor
├── XGBoostRegressor
└── LightGBMRegressor

Advanced Models:
├── CatBoostRegressor
├── ExtraTreesRegressor
└── ElasticNet (baseline)
```

**B. Hyperparameter Tuning:**

-   Grid Search cho baseline models
-   Random Search cho complex models
-   Bayesian Optimization cho production models
-   Cross-validation với time-series split

**C. Training Strategy:**

```
For each model type:
1. Baseline training với default params
2. Hyperparameter tuning
3. Feature selection optimization
4. Cross-validation evaluation
5. Final model training với best params
```

#### 2.6 MODEL EVALUATION LAYER

**Mục đích:** Comprehensive model assessment

**A. Performance Metrics:**

-   **Primary:** RMSE, MAE, MAPE
-   **Business:** Accuracy within 10%/20% range
-   **Statistical:** R², Adjusted R²
-   **Robustness:** Performance by price segments

**B. Validation Tests:**

-   **Temporal validation:** Recent data performance
-   **Geographical validation:** Performance by location
-   **Segment validation:** Performance by property type
-   **Outlier robustness:** Performance on edge cases

**C. Model Comparison:**

-   Performance ranking
-   Training time analysis
-   Memory usage assessment
-   Inference speed testing

#### 2.7 MODEL REGISTRY & VERSIONING

**Mục đích:** Quản lý model lifecycle

**A. Model Metadata:**

```
model_version:
├── model_id: unique identifier
├── training_date: when trained
├── data_period: data range used
├── features_used: feature list
├── hyperparameters: model config
├── performance_metrics: evaluation results
├── training_duration: time taken
└── data_quality_score: input data quality
```

**B. Versioning Strategy:**

-   **Major version:** Algorithm change
-   **Minor version:** Feature engineering change
-   **Patch version:** Hyperparameter tuning

**C. Model Stages:**

-   `development`: newly trained
-   `staging`: passed validation
-   `production`: serving traffic
-   `archived`: retired

#### 2.8 MODEL DEPLOYMENT LAYER

**Mục đích:** Deploy model vào production

**A. Deployment Options:**

-   **Batch Prediction:** Daily price estimates
-   **Real-time API:** On-demand predictions
-   **Streaming:** Live property analysis

**B. A/B Testing:**

-   Champion vs Challenger comparison
-   Traffic splitting
-   Performance monitoring

### 3. PIPELINE ORCHESTRATION

#### 3.1 AIRFLOW DAG ARCHITECTURE

```
ml_training_pipeline_dag:
├── validate_gold_data (30 min)
├── run_feature_engineering (60 min)
├── prepare_training_data (20 min)
├── train_models_parallel (120 min)
│   ├── train_random_forest
│   ├── train_gradient_boosting
│   ├── train_xgboost
│   └── train_lightgbm
├── evaluate_models (30 min)
├── select_best_model (10 min)
├── register_model (10 min)
└── deploy_to_staging (20 min)
```

#### 3.2 SCHEDULING STRATEGY

```
Daily Training (Incremental):
- Trigger: After Gold data ready (daily_processing_dag completes)
- Frequency: Daily at 6:00 AM
- Training data: Last 30 days rolling window

Weekly Full Retraining:
- Trigger: Sunday 2:00 AM
- Frequency: Weekly
- Training data: Last 90 days full dataset

Monthly Model Refresh:
- Trigger: 1st day of month, 4:00 AM
- Frequency: Monthly
- Training data: Last 180 days + hyperparameter retuning
```

### 4. DATA STORAGE ARCHITECTURE

#### 4.1 HDFS Structure

```
/hdfs/ml_pipeline/
├── features/
│   ├── {yyyy-mm-dd}/
│   │   ├── raw_features.parquet
│   │   ├── engineered_features.parquet
│   │   └── final_features.parquet
├── datasets/
│   ├── {yyyy-mm-dd}/
│   │   ├── train.parquet
│   │   ├── validation.parquet
│   │   └── test.parquet
├── models/
│   ├── {model_version}/
│   │   ├── model_artifacts/
│   │   ├── feature_pipeline/
│   │   └── metadata.json
└── evaluations/
    ├── {model_version}/
    │   ├── metrics.json
    │   ├── validation_report.html
    └── └── feature_importance.csv
```

#### 4.2 Model Registry Storage

```
/models/registry/
├── price_prediction/
│   ├── v1.0.0_20250605/
│   │   ├── model/
│   │   ├── metadata.json
│   │   └── metrics.json
│   ├── v1.1.0_20250612/
│   └── production -> v1.0.0_20250605/
```

### 5. MONITORING & ALERTING

#### 5.1 Data Quality Monitoring

-   Missing value trends
-   Distribution drift detection
-   Outlier frequency tracking
-   Source data availability

#### 5.2 Model Performance Monitoring

-   Prediction accuracy trends
-   Error distribution analysis
-   Feature importance changes
-   Inference latency tracking

#### 5.3 Pipeline Health Monitoring

-   Training job success rates
-   Resource utilization
-   Processing time trends
-   Error rate tracking

### 6. CONFIGURATION MANAGEMENT

#### 6.1 Environment Configuration

```
environments:
├── development: Local testing
├── staging: UAT environment
├── production: Live serving
```

#### 6.2 Feature Configuration

```yaml
feature_engineering:
    price_features:
        - price_log: true
        - price_per_sqm: true
        - price_percentile: true

    temporal_features:
        - season: true
        - month: true
        - days_since_crawl: true

    market_features:
        - local_average: true
        - price_deviation: true
        - supply_density: true
```

#### 6.3 Model Configuration

```yaml
models:
    random_forest:
        enabled: true
        n_estimators: [100, 200, 300]
        max_depth: [10, 15, 20]

    xgboost:
        enabled: true
        learning_rate: [0.01, 0.1, 0.2]
        max_depth: [6, 8, 10]
```

## NEXT STEPS - IMPLEMENTATION PLAN

### Phase 1: Foundation (Week 1-2)

1. ✅ Clean up existing ML code
2. 🔄 Implement data quality validation
3. 🔄 Enhanced feature engineering pipeline
4. 🔄 Data preparation utilities

### Phase 2: Core Training (Week 3-4)

1. Enhanced model training pipeline
2. Model evaluation framework
3. Model registry integration
4. Basic Airflow DAG

### Phase 3: Production Ready (Week 5-6)

1. Advanced model comparison
2. A/B testing framework
3. Monitoring & alerting
4. Full deployment pipeline

### Phase 4: Optimization (Week 7-8)

1. Hyperparameter optimization
2. Feature selection automation
3. Performance optimization
4. Advanced monitoring

---

**Câu hỏi cho bạn:**

1. Kiến trúc này có phù hợp với workflow hiện tại không?
2. Có feature nào bạn muốn thêm/bớt không?
3. Tần suất training (daily/weekly) có hợp lý không?
4. Cần ưu tiên implement phần nào trước?
