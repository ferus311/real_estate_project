# 📂 Standardized Path Structure Documentation

## 🎯 Overview

Tài liệu này mô tả **cấu trúc đường dẫn (paths) đã được chuẩn hóa** trong Real Estate Project để đảm bảo tính nhất quán across toàn bộ hệ thống.

## 🏗️ Kiến trúc Medallion Architecture

```
Raw Data → Bronze → Silver → Gold → ML Features → Serving
```

### **🔗 Data Flow với Paths:**

```
/data/realestate/raw/{source}/{property_type}/{yyyy/mm/dd}/
                  ↓
/data/realestate/processed/bronze/{source}/{property_type}/{yyyy/mm/dd}/
                  ↓
/data/realestate/processed/silver/{source}/{property_type}/{yyyy/mm/dd}/
                  ↓
/data/realestate/processed/gold/unified/{property_type}/{yyyy/mm/dd}/
                  ↓
/data/realestate/ml/features/{property_type}/{yyyy-mm-dd}/
                  ↓
/data/realestate/serving/api_data/{yyyy/mm/dd}/
```

## 📁 Unified Path Structure

### **1. Base Configuration**

```bash
# Root HDFS path
/data/realestate/
```

### **2. Data Processing Layers**

#### **Raw Data Layer**

```bash
# Crawler data (JSON files)
/data/realestate/raw/batdongsan/house/2025/06/06/
/data/realestate/raw/chotot/house/2025/06/06/
/data/realestate/raw/{source}/{property_type}/{yyyy/mm/dd}/
```

#### **Bronze Layer (Standardized)**

```bash
# Raw → Parquet conversion
/data/realestate/processed/bronze/batdongsan/house/2025/06/06/
/data/realestate/processed/bronze/chotot/house/2025/06/06/
/data/realestate/processed/bronze/{source}/{property_type}/{yyyy/mm/dd}/
```

#### **Silver Layer (Cleaned)**

```bash
# Data cleaning & validation
/data/realestate/processed/silver/batdongsan/house/2025/06/06/
/data/realestate/processed/silver/chotot/house/2025/06/06/
/data/realestate/processed/silver/{source}/{property_type}/{yyyy/mm/dd}/
```

#### **Gold Layer (Unified)**

```bash
# Multi-source unification
/data/realestate/processed/gold/unified/house/2025/06/06/
/data/realestate/processed/gold/unified/{property_type}/{yyyy/mm/dd}/

# Specific files:
/data/realestate/processed/gold/unified/house/2025/06/06/unified_house_20250606.parquet
```

### **3. ML & Analytics Layers**

#### **Feature Store**

```bash
# ML features
/data/realestate/ml/features/house/2025-06-06/
/data/realestate/ml/features/{property_type}/{yyyy-mm-dd}/
/data/realestate/ml/features/{feature_set}/{yyyy/mm/dd}/
```

#### **Model Registry**

```bash
# Trained models
/data/realestate/ml/models/price_prediction/v1.0.0/
/data/realestate/ml/models/{model_name}/{version}/
```

#### **Analytics**

```bash
# Market analysis
/data/realestate/analytics/market_trends/house/2025/06/06/
/data/realestate/analytics/{analysis_type}/{property_type}/{yyyy/mm/dd}/
```

### **4. Serving & Production**

#### **API Serving**

```bash
# API data cache
/data/realestate/serving/api_data/2025/06/06/
/data/realestate/serving/api_cache/{cache_key}/
/data/realestate/serving/{service_type}/{yyyy/mm/dd}/
```

### **5. Utility Paths**

#### **Temporary Processing**

```bash
/data/realestate/temp/{job_name}/{yyyy/mm/dd}/
/data/realestate/checkpoints/{job_name}/
```

#### **Backup & Archive**

```bash
/data/realestate/backup/{backup_type}/{yyyy/mm/dd}/
/data/realestate/archive/{archive_type}/{yyyy/mm/dd}/
```

## 🛠️ Implementation Guide

### **1. Import Unified Path Manager**

```python
from data_processing.common.config.unified_paths import path_manager

# Or for backward compatibility:
from data_processing.common.config.unified_paths import get_hdfs_path
```

### **2. Usage Examples**

#### **ETL Pipeline Paths:**

```python
# Raw data path
raw_path = path_manager.get_raw_path("batdongsan", "house", "2025-06-06")
# → /data/realestate/raw/batdongsan/house/2025/06/06/

# Bronze layer path
bronze_path = path_manager.get_bronze_path("batdongsan", "house", "2025-06-06")
# → /data/realestate/processed/bronze/batdongsan/house/2025/06/06/

# Silver layer path
silver_path = path_manager.get_silver_path("chotot", "house", "2025-06-06")
# → /data/realestate/processed/silver/chotot/house/2025/06/06/

# Gold unified path
gold_path = path_manager.get_gold_path("house", "2025-06-06")
# → /data/realestate/processed/gold/unified/house/2025/06/06/

# Specific unified file
unified_file = path_manager.get_unified_file_path("house", "2025-06-06")
# → /data/realestate/processed/gold/unified/house/2025/06/06/unified_house_20250606.parquet
```

#### **ML Pipeline Paths:**

```python
# Feature store path
features_path = path_manager.get_ml_features_path("house", "2025-06-06")
# → /data/realestate/ml/features/house/2025-06-06/

# Model registry path
model_path = path_manager.get_model_path("price_prediction", "v1.0.0")
# → /data/realestate/ml/models/price_prediction/v1.0.0/

# Analytics path
analytics_path = path_manager.get_analytics_path("market_trends", "house", "2025-06-06")
# → /data/realestate/analytics/market_trends/house/2025/06/06/
```

#### **Backward Compatibility:**

```python
# Existing code continues to work
from common.utils.date_utils import get_hdfs_path

bronze_path = get_hdfs_path(
    "/data/realestate/processed/bronze",
    "batdongsan",
    "house",
    "2025-06-06"
)
# → /data/realestate/processed/bronze/batdongsan/house/2025/06/06/
```

### **3. Date Format Standards**

```python
# Partition format (folders): yyyy/mm/dd
partition_date = path_manager.format_date_for_partition("2025-06-06")
# → "2025/06/06"

# Filename format: yyyymmdd
filename_date = path_manager.format_date_for_filename("2025-06-06")
# → "20250606"

# Hyphen format: yyyy-mm-dd
hyphen_date = path_manager.format_date_hyphen("2025-06-06")
# → "2025-06-06"
```

## 🔧 Migration Guide

### **Step 1: Update Imports**

Replace existing path generation with unified path manager:

```python
# OLD
from common.utils.date_utils import get_hdfs_path

# NEW
from data_processing.common.config.unified_paths import path_manager
```

### **Step 2: Update Path Generation**

```python
# OLD
gold_path = get_hdfs_path("/data/realestate/processed/gold/unified", property_type, date)

# NEW
gold_path = path_manager.get_gold_path(property_type, date)
```

### **Step 3: Validate Paths**

```python
# Check if path follows standards
is_valid = path_manager.validate_path_structure(some_path)

# Get all paths for debugging
all_paths = path_manager.get_all_paths_for_date("2025-06-06", "house")
for name, path in all_paths.items():
    print(f"{name}: {path}")
```

## ✅ Benefits của Standardization

### **1. Consistency**

-   **Unified naming**: Tất cả components sử dụng cùng path structure
-   **Predictable paths**: Dễ dàng tìm data theo pattern chuẩn
-   **Cross-component compatibility**: ETL, ML, Analytics đều follow cùng standard

### **2. Maintainability**

-   **Centralized configuration**: Chỉnh sửa path ở một nơi
-   **Version control**: Track path changes qua git
-   **Easy debugging**: Standard paths dễ troubleshoot

### **3. Scalability**

-   **New data sources**: Dễ dàng add thêm sources mới
-   **New layers**: Flexible để extend thêm processing layers
-   **Partition strategy**: Optimized cho performance & storage

## 🚨 Important Notes

### **1. Date Format Consistency**

-   **Folder partitions**: Always use `yyyy/mm/dd`
-   **File names**: Always use `yyyymmdd`
-   **API/ML inputs**: Use `yyyy-mm-dd`

### **2. Property Types**

-   **Standard values**: `house`, `other`, `all`
-   **Always lowercase**
-   **No spaces or special characters**

### **3. Source Names**

-   **Standard values**: `batdongsan`, `chotot`
-   **Always lowercase**
-   **Match crawler source identifiers**

### **4. Path Validation**

-   All paths must start with `/data/realestate/`
-   Use `path_manager.validate_path_structure()` to check
-   Log warnings for non-standard paths

## 📋 Migration Checklist

-   [ ] **Update imports** in all Python files
-   [ ] **Replace hardcoded paths** with path_manager calls
-   [ ] **Test backward compatibility** with existing code
-   [ ] **Update documentation** references
-   [ ] **Validate data accessibility** với new paths
-   [ ] **Update monitoring** và alerting systems
-   [ ] **Train team members** on new path standards

---

**📞 Support:** Contact Data Engineering team for migration assistance
**📚 Reference:** See `unified_paths.py` for complete API documentation
