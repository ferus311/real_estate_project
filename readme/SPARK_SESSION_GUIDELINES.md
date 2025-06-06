# Hướng dẫn thống nhất sử dụng Spark Session

## 🎯 **Quy tắc sử dụng**

### ✅ **ETL Workflows → `create_spark_session()`**

```python
from common.config.spark_config import create_spark_session

# ✅ Đúng cho ETL: extraction, transformation, unification, load
spark = create_spark_session("ETL Job Name")
```

### ✅ **ML Workflows → `create_optimized_ml_spark_session()`**

```python
from common.config.spark_config import create_optimized_ml_spark_session

# ✅ Đúng cho ML: feature engineering, model training, inference
spark = create_optimized_ml_spark_session("ML Job Name")
```

## ❌ **Những gì KHÔNG được làm**

### ❌ Tự tạo config riêng

```python
# ❌ SAI - Không tự tạo config
config = {
    "spark.sql.shuffle.partitions": "100",
    "spark.sql.adaptive.enabled": "true",
    # ... nhiều config khác
}
spark = create_spark_session("Job Name", config=config)
```

### ❌ Dùng sai hàm cho workflow

```python
# ❌ SAI - Dùng ETL session cho ML
spark = create_spark_session("ML Job")  # Thiếu tối ưu ML

# ❌ SAI - Dùng ML session cho ETL (overkill)
spark = create_optimized_ml_spark_session("ETL Job")  # Thừa tài nguyên
```

## 📋 **Mapping workflow → session type**

| Workflow Type | Function                              | Files                                                                                              |
| ------------- | ------------------------------------- | -------------------------------------------------------------------------------------------------- |
| **ETL**       | `create_spark_session()`              | `pipelines/daily_processing.py`<br>`jobs/extraction/*`<br>`jobs/transformation/*`<br>`jobs/load/*` |
| **ML**        | `create_optimized_ml_spark_session()` | `pipelines/ml_processing.py`<br>`jobs/enrichment/ml_*`<br>`ml/*`                                   |

## 🔧 **Đã tối ưu**

### ✅ Loại bỏ code trùng lặp

-   Xóa config riêng lẻ trong từng file
-   Centralized configuration trong `spark_config.py`
-   Thống nhất cách import và sử dụng

### ✅ Phân tách rõ ràng

-   **ETL Session**: Memory = 2g/4g, Partitions = 50
-   **ML Session**: Memory = 3g/6g, Partitions = 100, Arrow enabled, ML optimizations

## 🚀 **Kết quả**

-   **Consistency**: Tất cả files đều dùng cùng pattern
-   **Performance**: Mỗi workflow có cấu hình tối ưu riêng
-   **Maintainability**: Chỉ sửa 1 chỗ trong `spark_config.py`
-   **No Duplication**: Không còn config scattered khắp nơi
