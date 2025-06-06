# Pipeline Separation Guide

## Overview

The original monolithic pipeline has been successfully separated into two focused, independent pipelines:

### 1. ETL Pipeline (`daily_processing.py`)

**Focus**: Raw → Bronze → Silver → Gold → Serving

-   **Purpose**: Data extraction, transformation, and serving preparation
-   **Frequency**: Daily execution
-   **Resources**: Standard Spark configuration
-   **Output**: Clean, unified data ready for serving and ML consumption

### 2. ML Pipeline (`ml_processing.py`)

**Focus**: Gold → ML Features → Trained Models

-   **Purpose**: Machine learning feature engineering and model training
-   **Frequency**: Weekly/Monthly execution
-   **Resources**: ML-optimized Spark configuration
-   **Input**: Requires Gold data from ETL pipeline

## Architecture Benefits

### 🔄 **Separation of Concerns**

-   ETL pipeline focuses purely on data processing
-   ML pipeline focuses purely on model development
-   Clear boundaries and responsibilities

### 📈 **Independent Scaling**

-   ETL: Optimized for data throughput
-   ML: Optimized for compute-intensive operations
-   Different resource requirements can be met

### ⏰ **Flexible Scheduling**

-   ETL: Daily automated runs
-   ML: Can run weekly, monthly, or on-demand
-   No unnecessary resource consumption

### 🔧 **Easier Maintenance**

-   Smaller, focused codebases
-   Independent testing and debugging
-   Separate team ownership possible

## Usage Examples

### ETL Pipeline Usage

#### Daily Full Pipeline

```bash
# Run complete ETL pipeline for today
python daily_processing.py

# Run for specific date
python daily_processing.py --date 2025-06-01

# Run for specific property types
python daily_processing.py --date 2025-06-01 --property-types house other
```

#### Stage-Specific Execution

```bash
# Run only extraction stage
python daily_processing.py --extract-only --date 2025-06-01

# Run only transformation stage
python daily_processing.py --transform-only --date 2025-06-01

# Run only unification stage
python daily_processing.py --unify-only --date 2025-06-01

# Run only load stage
python daily_processing.py --load-only --date 2025-06-01
```

#### Skip Stages

```bash
# Skip extraction (use existing Bronze data)
python daily_processing.py --skip-extraction --date 2025-06-01

# Skip transformation (start from Silver data)
python daily_processing.py --skip-transformation --date 2025-06-01

# Run only extraction and transformation
python daily_processing.py --skip-unification --skip-load --date 2025-06-01
```

#### Load Targets

```bash
# Load only to PostgreSQL
python daily_processing.py --load-targets postgres --date 2025-06-01

# Load to both Delta and PostgreSQL
python daily_processing.py --load-targets both --date 2025-06-01
```

### ML Pipeline Usage

#### Full ML Pipeline

```bash
# Run complete ML pipeline for today (requires Gold data)
python ml_processing.py

# Run for specific date
python ml_processing.py --date 2025-06-01

# Run for specific property types
python ml_processing.py --date 2025-06-01 --property-types house other
```

#### Stage-Specific Execution

```bash
# Run only feature engineering
python ml_processing.py --feature-only --date 2025-06-01

# Run only model training (requires ML features)
python ml_processing.py --training-only --date 2025-06-01
```

#### Skip Stages

```bash
# Skip feature engineering (use existing ML features)
python ml_processing.py --skip-features --date 2025-06-01

# Skip training (only generate features)
python ml_processing.py --skip-training --date 2025-06-01
```

## Typical Workflow

### Daily Operations

```bash
# 1. Run ETL pipeline daily (automated via Airflow)
python daily_processing.py --date $(date +%Y-%m-%d)
```

### Weekly ML Updates

```bash
# 2. Run ML pipeline weekly (after collecting enough Gold data)
python ml_processing.py --date $(date +%Y-%m-%d)
```

### Development & Testing

```bash
# Test ETL pipeline for specific date
python daily_processing.py --date 2025-06-01 --property-types house

# Test only new feature engineering
python ml_processing.py --feature-only --date 2025-06-01 --property-types house

# Test only model training with existing features
python ml_processing.py --training-only --date 2025-06-01 --property-types house
```

### Data Recovery

```bash
# Re-run failed ETL stage
python daily_processing.py --transform-only --date 2025-06-01

# Re-run failed ML training only
python ml_processing.py --training-only --date 2025-06-01
```

## Configuration Differences

### ETL Pipeline (`daily_processing.py`)

-   **Spark Config**: Standard data processing configuration
-   **Memory**: Moderate memory allocation
-   **Focus**: Data throughput and transformation speed

### ML Pipeline (`ml_processing.py`)

-   **Spark Config**: ML-optimized configuration
-   **Memory**: Higher memory allocation for model training
-   **Focus**: Compute-intensive ML operations
-   **Features**: Broadcast optimization, ML-specific caching

## Data Dependencies

```
ETL Pipeline:
Raw Data → Bronze → Silver → Gold → PostgreSQL Serving

ML Pipeline:
Gold Data (from ETL) → ML Features → Trained Models
```

**Important**: ML pipeline requires Gold data to exist. Always ensure ETL pipeline has run successfully before executing ML pipeline.

## Monitoring & Logging

Both pipelines use the same logging infrastructure but with different logger names:

-   ETL Pipeline: `etl_processing_pipeline`
-   ML Pipeline: `ml_processing_pipeline`

This allows for separate monitoring and alerting strategies.

## Migration Notes

-   ✅ All ML-related code removed from `daily_processing.py`
-   ✅ Dedicated `ml_processing.py` created with ML-optimized configuration
-   ✅ Argument parsing updated for each pipeline's specific needs
-   ✅ Clear separation of concerns maintained
-   ✅ Both pipelines can run independently

## Future Enhancements

1. **Airflow DAGs**: Create separate DAGs for ETL and ML pipelines
2. **Resource Allocation**: Configure different cluster resources for each pipeline
3. **Data Validation**: Add data quality checks between pipeline stages
4. **Monitoring**: Implement separate monitoring dashboards for ETL vs ML metrics
