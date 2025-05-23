#!/bin/bash
# filepath: /home/fer/data/real_estate_project/data_processing/spark/entrypoint.sh

set -e

# Prints làm dấu hiệu để log theo dõi trong Docker logs
echo "======================================================"
echo "Starting Spark processing job"
echo "======================================================"
echo "Current directory: $(pwd)"
echo "Job script: $1"
echo "All arguments: $@"
echo "======================================================"

# Kiểm tra xem tham số có phải là đường dẫn đầy đủ không
if [[ "$1" == /* ]]; then
    JOB_PATH="$1"
    shift
else
    # Nếu không phải là đường dẫn đầy đủ, ta phải tìm đường dẫn phù hợp
    if [[ -f "/app/jobs/$1" ]]; then
        JOB_PATH="/app/jobs/$1"
        shift
    elif [[ -f "/app/pipelines/$1" ]]; then
        JOB_PATH="/app/pipelines/$1"
        shift
    elif [[ -f "/app/$1" ]]; then
        JOB_PATH="/app/$1"
        shift
    else
        # Mặc định là daily_processing.py trong pipelines
        JOB_PATH="/app/pipelines/daily_processing.py"
    fi
fi

# Kiểm tra xem biến môi trường SPARK_MASTER_URL có được thiết lập không
if [ -z "${SPARK_MASTER_URL}" ]; then
    # Nếu không có, sử dụng local[*]
    SPARK_MASTER="local[*]"
else
    # Nếu có, sử dụng giá trị của biến môi trường
    SPARK_MASTER="${SPARK_MASTER_URL}"
fi

# Thiết lập các tham số mặc định cho spark-submit
SPARK_SUBMIT_ARGS=(
    "--master" "${SPARK_MASTER}"
    "--deploy-mode" "client"
    "--conf" "spark.hadoop.fs.defaultFS=hdfs://namenode:9000"
)

# Thêm các tham số khác nếu được thiết lập trong biến môi trường
if [ ! -z "${SPARK_DRIVER_MEMORY}" ]; then
    SPARK_SUBMIT_ARGS+=("--driver-memory" "${SPARK_DRIVER_MEMORY}")
fi

if [ ! -z "${SPARK_EXECUTOR_MEMORY}" ]; then
    SPARK_SUBMIT_ARGS+=("--executor-memory" "${SPARK_EXECUTOR_MEMORY}")
fi

# Kiểm tra xem có tham số "--master" đã được truyền vào không
MASTER_SPECIFIED=false
for arg in "$@"; do
    if [[ "$arg" == "--master" ]]; then
        MASTER_SPECIFIED=true
        break
    fi
done

# Nếu không có tham số --master được truyền vào, sử dụng SPARK_SUBMIT_ARGS
if [ "$MASTER_SPECIFIED" = false ]; then
    echo "Using default Spark master: ${SPARK_MASTER}"
    # Thêm tên tệp job và các tham số được truyền vào
    exec spark-submit "${SPARK_SUBMIT_ARGS[@]}" "$JOB_PATH" "$@"
else
    echo "Using provided Spark master parameters"
    # Sử dụng tham số được truyền vào
    exec spark-submit "$JOB_PATH" "$@"
fi
