# Daily Processing Pipeline with Stage Control

Pipeline xử lý dữ liệu bất động sản hàng ngày với khả năng chạy từng stage riêng biệt để debug dễ hơn.

## 🏗️ Architecture

```
Raw Data → Bronze → Silver → Gold
    ↓        ↓       ↓       ↓
Extract  Transform  Unify  Final
```

### Data Flow:

-   **Raw → Bronze**: Extract dữ liệu từ JSON files sang Parquet
-   **Bronze → Silver**: Transform và standardize data
-   **Silver → Gold**: Unify data từ nhiều sources

### Storage Paths:

-   **Bronze**: `/data/realestate/processed/bronze/{source}/{property_type}/{date}/`
-   **Silver**: `/data/realestate/processed/silver/{source}/{property_type}/{date}/`
-   **Gold**: `/data/realestate/processed/gold/unified/{property_type}/{date}/`

## 🚀 Usage

### Basic Usage

```bash
# Run full pipeline for yesterday
python daily_processing.py

# Run for specific date
python daily_processing.py --date 2024-01-15

# Run for specific property types
python daily_processing.py --property-types house other
```

### Stage Control Options

#### Run Single Stage Only

```bash
# Only extraction (Raw → Bronze)
python daily_processing.py --extract-only --date 2024-01-15

# Only transformation (Bronze → Silver)
python daily_processing.py --transform-only --date 2024-01-15

# Only unification (Silver → Gold)
python daily_processing.py --unify-only --date 2024-01-15
```

#### Skip Specific Stages

```bash
# Skip extraction (useful when bronze data already exists)
python daily_processing.py --skip-extraction --date 2024-01-15

# Skip transformation (run extraction + unification only)
python daily_processing.py --skip-transformation --date 2024-01-15

# Skip unification (run extraction + transformation only)
python daily_processing.py --skip-unification --date 2024-01-15
```

#### Combined Options

```bash
# Run extraction + transformation only
python daily_processing.py --skip-unification --date 2024-01-15

# Run transformation + unification only
python daily_processing.py --skip-extraction --date 2024-01-15
```

## 🐛 Debugging Workflow

### 1. When Pipeline Fails

```bash
# First, try extraction only to isolate issue
python daily_processing.py --extract-only --date 2024-01-15

# If extraction succeeds, try transformation
python daily_processing.py --transform-only --date 2024-01-15

# Finally, try unification
python daily_processing.py --unify-only --date 2024-01-15
```

### 2. Common Debug Scenarios

#### Schema Inference Error in Unification

```bash
# Check if silver data exists and has correct schema
python daily_processing.py --transform-only --date 2024-01-15

# Then try unification separately
python daily_processing.py --unify-only --date 2024-01-15
```

#### CodeGenerator Compilation Error

```bash
# Try with smaller dataset first
python daily_processing.py --extract-only --property-types house --date 2024-01-15
python daily_processing.py --transform-only --property-types house --date 2024-01-15
```

#### Memory Issues

```bash
# Process one property type at a time
python daily_processing.py --property-types house --date 2024-01-15
python daily_processing.py --property-types other --date 2024-01-15
```

## 📊 Data Validation

Pipeline tự động kiểm tra:

-   ✅ Bronze data availability before transformation
-   ✅ Silver data availability before unification
-   ✅ Record counts at each stage
-   ✅ Processing duration tracking

## 🔧 Interactive Demo

Run the demo script để test các stage options:

```bash
python run_stages_demo.py
```

## 📝 Logs

Logs được lưu tại: `/tmp/logs/daily_processing_{YYYYMMDD}/`

Log levels:

-   **INFO**: Normal processing info
-   **WARNING**: Non-critical issues (e.g., missing data from one source)
-   **ERROR**: Critical failures that stop processing

## ⚡ Performance Tips

1. **For debugging**: Sử dụng single stage options
2. **For production**: Run full pipeline
3. **For reprocessing**: Skip stages khi data đã tồn tại
4. **For memory issues**: Process từng property type riêng biệt

## 🚨 Error Handling

Pipeline sẽ:

-   ✅ Continue processing other property types nếu một type fails
-   ✅ Validate data availability trước khi chạy từng stage
-   ✅ Provide detailed error messages với context
-   ✅ Clean up memory với unpersist() calls
-   ✅ Return proper exit codes (0 = success, 1 = failure)

## 📋 Arguments Reference

| Argument                | Type   | Default   | Description                   |
| ----------------------- | ------ | --------- | ----------------------------- |
| `--date`                | string | yesterday | Processing date (YYYY-MM-DD)  |
| `--property-types`      | list   | ["house"] | Property types: house, other  |
| `--extract-only`        | flag   | False     | Only run extraction stage     |
| `--transform-only`      | flag   | False     | Only run transformation stage |
| `--unify-only`          | flag   | False     | Only run unification stage    |
| `--skip-extraction`     | flag   | False     | Skip extraction stage         |
| `--skip-transformation` | flag   | False     | Skip transformation stage     |
| `--skip-unification`    | flag   | False     | Skip unification stage        |

⚠️ **Note**: `--extract-only`, `--transform-only`, `--unify-only` are mutually exclusive.
